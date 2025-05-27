# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 1.2                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "21:50 - 20250527"                    #
#                                                       #
#  Credits:                                             #
#  - Original concept by Lululla                        #
#  Usage of this code without proper attribution        #
#  is strictly prohibited.                              #
#  For modifications and redistribution,                #
#  please maintain this credit header.                  #
#########################################################
"""
__author__ = "Lululla"

import logging
import codecs
import glob
from gettext import gettext as _
from os import access, listdir, makedirs, remove, replace, chmod, fsync, W_OK
from os.path import basename, dirname, exists, isdir, isfile, join, normpath
from re import sub, findall, DOTALL, search
from urllib.parse import quote, urlparse, unquote
from twisted.internet import threads
from Components.ActionMap import ActionMap
from Components.FileList import FileList
from Components.Label import Label
from Components.MenuList import MenuList
from Components.Sources.Progress import Progress
from Components.Sources.StaticText import StaticText
from Components.config import config, ConfigSelection, ConfigSubsection, ConfigYesNo
from Screens.MessageBox import MessageBox
from Screens.Screen import Screen
from Screens.Setup import Setup
from Tools.Directories import fileExists, resolveFilename, SCOPE_MEDIA
from enigma import eDVBDB
import unicodedata


# for my friend Archimede@
currversion = '1.2'

logger = logging.getLogger("UniversalConverter")

# Init Config
config.plugins.m3uconverter = ConfigSubsection()
config.plugins.m3uconverter.lastdir = ConfigSelection(default="/media/hdd", choices=[])
config.plugins.m3uconverter.bouquet_position = ConfigSelection(
	default="bottom",
	choices=[("top", _("Top")), ("bottom", _("Bottom"))]
)
config.plugins.m3uconverter.hls_convert = ConfigYesNo(default=True)
config.plugins.m3uconverter.auto_reload = ConfigYesNo(default=True)
config.plugins.m3uconverter.backup_enable = ConfigYesNo(default=True)


class M3UFileBrowser(Screen):
	def __init__(self, session, startdir="/media/hdd", matchingPattern=r"(?i)^.*\.m3u$"):
		Screen.__init__(self, session)
		self.skinName = "FileBrowser"

		self["title"] = Label("Select file")
		self["filelist"] = FileList(
			startdir,
			matchingPattern=matchingPattern,
			showDirectories=True,
			showFiles=True,
			useServiceRef=False
		)

		self["actions"] = ActionMap(["OkCancelActions", "ColorActions"], {
			"ok": self.ok_pressed,
			# "OK": self.ok_pressed, (.. on Atv are OK Uppercase in keymap.xml ??
			"green": self.ok_pressed,
			"cancel": self.close
		}, -1)

	def ok_pressed(self):
		selection = self["filelist"].getCurrent()
		if not selection:
			return

		# Get the path from the tuple
		first_item = selection[0]
		if not isinstance(first_item, tuple):
			return

		path = first_item[0]
		is_dir = first_item[1]

		if not isinstance(path, str):
			return

		# Ignore "<Top folder>" if user just entered a folder
		if path.endswith("/") and path.count("/") == 3:
			# likely means '/media/hdd/movie/' → '/media/hdd/' → prevent loop
			return

		if is_dir:
			self["filelist"].changeDir(path)
		else:
			self.close(path)


def create_backup():
	"""Create a backup of bouquets.tv only"""
	from shutil import copy2
	src = "/etc/enigma2/bouquets.tv"
	dst = "/etc/enigma2/bouquets.tv.bak"
	if exists(src):
		copy2(src, dst)


def reload_services():
	"""Reload the list of services"""
	eDVBDB.getInstance().reloadServicelist()
	eDVBDB.getInstance().reloadBouquets()


def transliterate(text):
	normalized = unicodedata.normalize("NFKD", text)
	return normalized.encode('ascii', 'ignore').decode('ascii')


def clean_group_name(name):
	return name.encode("ascii", "ignore").decode().replace(" ", "_").replace("/", "_").replace(":", "_")[:50]


class ConversionSelector(Screen):
	skin = """
	<screen position="center,center" size="1280,720" title="Archimede Universal Converter - Select Type">
		<widget name="list" position="20,20" size="1240,559" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
		<widget name="status" position="20,630" size="1240,50" font="Regular;24" />
		<ePixmap position="20,685" size="200,40" pixmap="skin_default/buttons/red.png" />
		<ePixmap position="280,685" size="200,40" pixmap="skin_default/buttons/green.png" />
		<ePixmap position="538,685" size="200,40" pixmap="skin_default/buttons/yellow.png" />
		<!--
		<ePixmap position="817,685" size="200,40" pixmap="skin_default/buttons/blue.png" />
		-->
		<widget source="key_red" render="Label" position="73,685" size="200,40" zPosition="1" font="Regular;24" />
		<widget source="key_green" render="Label" position="326,685" size="200,40" zPosition="1" font="Regular;24" />
		<widget source="key_yellow" render="Label" position="600,685" size="200,40" zPosition="1" font="Regular;24" />
		<!--
		<widget source="key_blue" render="Label" position="880,685" size="200,40" zPosition="1" font="Regular;24" />
		-->
	</screen>"""

	def __init__(self, session):
		Screen.__init__(self, session)
		self.session = session
		self.skinName = "ConversionSelector"
		self.is_modal = True

		self.menu = [
			(_("Convert M3U to Enigma2 Bouquets"), "m3u_to_tv"),
			(_("Convert Enigma2 Bouquets to M3U"), "tv_to_m3u"),
			(_("Remove Enigma2 Bouquets Created with M3UConverter"), "purge_m3u_bouquets"),
		]
		self["list"] = MenuList(self.menu)
		self["status"] = Label(_("Select Type of Conversion"))
		self["actions"] = ActionMap(["ColorActions", "OkCancelActions"], {
			"red": self.close,
			"green": self.select_item,
			"ok": self.select_item,
			"yellow": self.purge_m3u_bouquets,
			"cancel": self.close
		})
		self["key_red"] = StaticText(_("Close"))
		self["key_green"] = StaticText(_("Select"))
		self["key_yellow"] = StaticText(_("Remove Bouquets"))

	def purge_m3u_bouquets(self, directory="/etc/enigma2", pattern="_m3ubouquet.tv"):
		"""Remove all bouquet files created by M3UConverter"""
		create_backup()
		removed_files = []

		# Remove matching bouquet files
		for f in listdir(directory):
			file_path = join(directory, f)
			if isfile(file_path) and search(pattern, f):
				try:
					remove(file_path)
					removed_files.append(f)
				except Exception as e:
					logger.error("Failed to remove %s: %s" % (f, str(e)))

		# Remove matching lines from bouquets.tv
		bouquets_file = join(directory, "bouquets.tv")
		if exists(bouquets_file):
			with open(bouquets_file, "r", encoding="utf-8") as f:
				lines = f.readlines()
			with open(bouquets_file, "w", encoding="utf-8") as f:
				for line in lines:
					if not search(pattern, line):
						f.write(line)

		if removed_files:
			message = "Removed %d bouquet(s):\n%s" % (
				len(removed_files),
				"\n".join(removed_files)
			)
		else:
			message = "No M3UConverter bouquets found to remove."

		self.session.open(MessageBox, message, MessageBox.TYPE_INFO)

	def select_item(self):
		selection = self["list"].getCurrent()
		if selection[1] == "purge_m3u_bouquets":
			self.purge_m3u_bouquets()
			return

		elif selection:
			self.close(selection[1])


class UniversalConverter(Screen):
	skin = """
	<screen position="center,center" size="1280,720" title="Archimede Universal Converter">
		<widget name="list" position="20,20" size="1240,559" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
		<widget name="status" position="20,630" size="1240,50" font="Regular;24" />
		<ePixmap position="20,685" size="200,40" pixmap="skin_default/buttons/red.png" />
		<ePixmap position="280,685" size="200,40" pixmap="skin_default/buttons/green.png" />
		<ePixmap position="538,685" size="200,40" pixmap="skin_default/buttons/yellow.png" />
		<ePixmap position="817,685" size="200,40" pixmap="skin_default/buttons/blue.png" />
		<widget source="key_red" render="Label" position="73,685" size="200,40" zPosition="1" font="Regular;24" />
		<widget source="key_green" render="Label" position="326,685" size="200,40" zPosition="1" font="Regular;24" />
		<widget source="key_yellow" render="Label" position="600,685" size="200,40" zPosition="1" font="Regular;24" />
		<widget source="key_blue" render="Label" position="880,685" size="200,40" zPosition="1" font="Regular;24" />
		<widget source="progress_source" render="Progress" position="51,589" size="1180,30" />
		<widget source="progress_text" render="Label" position="49,587" size="1180,30" font="Regular;24" />
	</screen>"""

	def __init__(self, session, conversion_type):
		Screen.__init__(self, session)
		self.conversion_type = conversion_type
		self.m3u_list = []
		self.bouquet_list = []
		self.selected_file = ""
		self.progress = None
		self.progress_source = Progress()
		base_path = config.plugins.m3uconverter.lastdir.value
		self.full_path = base_path
		self["list"] = MenuList([])
		self["status"] = Label(_("Ready"))
		self["key_red"] = StaticText(_("Open"))
		self["key_green"] = StaticText(_("Convert"))
		self["key_yellow"] = StaticText(_("Settings"))
		self["key_blue"] = StaticText(_("Info"))
		self["progress_source"] = self.progress_source
		self["progress_text"] = StaticText("")
		self["progress_source"].setValue(0)
		self["actions"] = ActionMap(["ColorActions", "OkCancelActions"], {
			"red": self.open_file,
			"green": self.start_conversion,
			"yellow": self.open_settings,
			"blue": self.show_credits,
			"cancel": self.close
		}, -1)

		if self.conversion_type == "m3u_to_tv":
			self.init_m3u_converter()
		else:
			self.init_tv_converter()

	def init_m3u_converter(self):
		self["key_green"].setText(_("Convert M3U"))
		self["status"].setText(_("Press OK to select M3U file"))
		self.onShown.append(self.delayed_file_browser)

	def delayed_file_browser(self):
		self.onShown.remove(self.delayed_file_browser)
		self.open_file_browser()

	def init_tv_converter(self):
		self["key_green"].setText(_("Convert Bouquet"))
		self.update_path_tv()

	def update_path_tv(self):
		try:
			if not exists("/etc/enigma2"):
				raise OSError("Bouquets path not found")

			if not access("/etc/enigma2", W_OK):
				logger.warning("Read-only bouquets path")

		except Exception as e:
			logger.error("TV path error: %s", str(e))
			self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR)

	def load_bouquets(self):
		"""Load only active IPTV bouquets with HTTP streams"""
		self.bouquet_list = []

		bouquet_tv_path = "/etc/enigma2/bouquets.tv"
		if not exists(bouquet_tv_path):
			return

		with open(bouquet_tv_path, "r", encoding="utf-8") as f:
			lines = f.readlines()

		# Parse only active bouquet paths
		bouquet_files = []
		for line in lines:
			if line.startswith("#SERVICE") and "FROM BOUQUET" in line:
				parts = line.split('"')
				if len(parts) >= 2:
					filename = parts[1]
					if filename.endswith(".tv"):
						bouquet_files.append("/etc/enigma2/" + filename)

		# Filter only those with http streams
		for bouquet in bouquet_files:
			if not exists(bouquet):
				continue
			try:
				with open(bouquet, "r", encoding="utf-8") as f:
					content = f.read()
					if "http" in content.lower():
						self.bouquet_list.append(bouquet)
			except Exception as e:
				_log_error("Failed to read bouquet: " + bouquet + " - " + str(e))

		self["list"].setList([basename(b) for b in self.bouquet_list])

	def open_file_browser(self):
		if self.conversion_type == "m3u_to_tv":
			pattern = r"(?i)^.*\.m3u$"
		else:
			pattern = r"(?i)^.*\.tv$"

		self.session.openWithCallback(
			self.file_selected,
			M3UFileBrowser,
			config.plugins.m3uconverter.lastdir.value,
			matchingPattern=pattern
		)

	def start_conversion(self):
		if self.conversion_type == "m3u_to_tv":
			self.update_mounts()
			self.update_path()
			self.convert_m3u_to_tv()
		else:
			self.convert_tv_to_m3u()

	def convert_m3u_to_tv(self):

		def conversion_task():
			try:
				groups = {}
				for idx, channel in enumerate(self.m3u_list):
					group = channel.get('group', 'Default')
					groups.setdefault(group, []).append(channel)
					self.update_progress(idx + 1, _("Processing: %s") % channel['name'])

				# First write a group
				for group, channels in groups.items():
					self.write_group_bouquet(group, channels)

				# Next update main bouquet
				self.update_main_bouquet(groups.keys())

				return (True, len(self.m3u_list))
			except Exception as e:
				return (False, str(e))

		threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

	def update_mounts(self):
		"""Update the list of mounted devices"""
		mounts = self.get_mounted_devices()
		config.plugins.m3uconverter.lastdir.setChoices(mounts)
		config.plugins.m3uconverter.lastdir.save()

	def get_mounted_devices(self):
		"""Recovers mounted devices with write permissions"""
		from Components.Harddisk import harddiskmanager

		devices = [
			(resolveFilename(SCOPE_MEDIA, 'hdd'), _("Hard Disk")),
			(resolveFilename(SCOPE_MEDIA, 'usb'), _("USB Drive"))
		]

		try:
			devices += [
				(p.mountpoint, p.description or _("Disk"))
				for p in harddiskmanager.getMountedPartitions()
				if p.mountpoint and access(p.mountpoint, W_OK)
			]

			net_dir = resolveFilename(SCOPE_MEDIA, 'net')
			if isdir(net_dir):
				devices += [(join(net_dir, d), _("Network")) for d in listdir(net_dir)]

		except Exception as e:
			logger.error(f"Mount error: {str(e)}")

		return [(p.rstrip('/') + '/', d) for p, d in devices if isdir(p)]

	def update_path(self):
		"""Update path with special handling for /tmp and ensure it's a valid directory."""
		try:
			if self.conversion_type == "tv_to_m3u":
				self.full_path = "/etc/enigma2"
				if not isdir(self.full_path):
					logger.warning("Path %s does not exist, falling back to /tmp", self.full_path)
					self.full_path = "/tmp"
			else:
				base_path = config.plugins.m3uconverter.lastdir.value

				if not base_path or not isdir(base_path):
					fallbacks = ["/media/hdd", "/media/usb", "/tmp"]
					base_path = next((p for p in fallbacks if isdir(p)), "/tmp")

				if base_path == "/tmp":
					self.full_path = base_path
				else:
					self.full_path = join(base_path, "movie")
					if not isdir(self.full_path):
						makedirs(self.full_path, exist_ok=True)
						chmod(self.full_path, 0o755)

			if not isdir(self.full_path):
				logger.error("Final path %s is not a directory, falling back to /tmp", self.full_path)
				self.full_path = "/tmp"

			logger.info("Using path: %s", self.full_path)

		except Exception as e:
			logger.error("Path update failed: %s", str(e))
			self.full_path = "/tmp"

	def open_file(self):
		"""Opens the selector file without selectedItems parameter"""
		try:
			self.update_path()

			if self.conversion_type == "tv_to_m3u":
				pattern = r"(?i)^.*\.tv$"
			else:
				pattern = r"(?i)^.*\.m3u$"

			self.session.openWithCallback(
				self.file_selected,
				M3UFileBrowser,
				self.full_path,
				matchingPattern=pattern
			)
		except Exception as e:
			logger.error(f"Browser error: {str(e)}")
			self.session.open(
				MessageBox,
				_("Error browser file:\n%s") % str(e),
				MessageBox.TYPE_ERROR
			)

	def file_selected(self, res=None):
		logger.info("Callback file_selected: %s", str(res))

		if not res:
			self["status"].setText(_("No files selected"))
			return
		logger.info("Callback file_selected: %s" % str(res))
		# If res is tuple or list, take the first element
		selected_path = None
		if isinstance(res, (tuple, list)):
			if len(res) == 0:
				self["status"].setText(_("No files selected"))
				return
			selected_path = res[0]
		elif isinstance(res, str):
			selected_path = res
		else:
			self["status"].setText(_("Invalid selection"))
			return

		if not fileExists(selected_path):
			self["status"].setText(_("Selected file does not exist"))
			return

		try:
			selected_path = normpath(selected_path)

			# Extension validation based on conversion type
			if self.conversion_type == "m3u_to_tv":
				if not selected_path.lower().endswith('.m3u'):
					raise ValueError(_("Select a valid M3U file"))
			else:
				if not selected_path.lower().endswith('.tv'):
					raise ValueError(_("Select a valid TV bouquet"))

			# Update route configuration
			config_dir = dirname(selected_path)
			if isdir(config_dir):
				config.plugins.m3uconverter.lastdir.value = config_dir
				config.plugins.m3uconverter.lastdir.save()

			self.selected_file = selected_path

			# Start appropriate parsing
			if self.conversion_type == "m3u_to_tv":
				self.parse_m3u(selected_path)
			else:
				self.parse_tv(selected_path)

			self["status"].setText(_("Selected file: %s") % selected_path)

		except Exception as e:
			logger.error("Error file selected: %s", str(e), exc_info=True)
			self["status"].setText(_("Error: %s") % str(e))
			self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR)

	def parse_m3u(self, filename):
		"""Analyze M3U files with advanced attribute management"""
		try:
			with open(filename, 'r', encoding='utf-8', errors='replace') as f:
				data = f.read()

			self.m3u_list = []
			self.filename = filename

			# 1. Initial backup
			if config.plugins.m3uconverter.backup_enable.value:
				create_backup()

			channels = self._parse_m3u_content(data)
			# Advanced Attribute Mapping
			for channel in channels:
				self.m3u_list.append({
					'name': channel.get('title', ''),
					'group': channel.get('group-title', ''),
					'tvg_name': channel.get('tvg-name', ''),
					'logo': channel.get('tvg-logo', ''),
					'url': self.process_url(channel.get('uri', '')),
					'duration': channel.get('length', ''),
					'user_agent': channel.get('user_agent', ''),
					'program_id': channel.get('program-id', '')
				})

			# UI Update
			display_list = []
			for c in self.m3u_list:
				group = c['group']
				if group:
					name = sub(r'\[.*?\]', '', c['name']).strip()

				else:
					name = sub(r'\[.*?\]', '', c['name']).strip()

				if group:
					display_list.append(group + " - " + name)
				else:
					display_list.append(name)

			self["list"].setList(display_list)
			self["status"].setText(_("Loaded %d channels") % len(self.m3u_list))

		except Exception as e:
			logger.error(f"Error parsing M3U: {str(e)}")
			self.session.open(
				MessageBox,
				_("Invalid file format:\n%s") % str(e),
				MessageBox.TYPE_ERROR
			)

	def _parse_m3u_content(self, data):
		"""Advanced parser for M3U content"""

		def get_attributes(txt, first_key_as_length=False):
			attribs = {}
			current_key = ''
			current_value = ''
			parse_state = 0  # 0=key, 1=value
			txt = txt.strip()

			for char in txt:
				if parse_state == 0:
					if char == '=':
						parse_state = 1
						if first_key_as_length and not attribs:
							attribs['length'] = current_key.strip()
							current_key = ''
						else:
							current_key = current_key.strip()
					else:
						current_key += char
				elif parse_state == 1:
					if char == '"':
						if current_value:
							attribs[current_key] = current_value
							current_key = ''
							current_value = ''
							parse_state = 0
						else:
							parse_state = 2
					else:
						current_value += char
				elif parse_state == 2:
					if char == '"':
						attribs[current_key] = current_value
						current_key = ''
						current_value = ''
						parse_state = 0
					else:
						current_value += char

			return attribs

		entries = []
		current_params = {}
		data = data.replace("\r\n", "\n").replace("\r", "\n").split("\n")

		for line in data:
			line = line.strip()
			if not line:
				continue

			if line.startswith('#EXTINF:'):
				current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
				parts = line[8:].split(',', 1)
				if len(parts) > 1:
					current_params['title'] = parts[1].strip()
				attribs = get_attributes(parts[0], first_key_as_length=True)
				current_params.update(attribs)

			elif line.startswith('#EXTGRP:'):
				current_params['group-title'] = line[8:].strip()

			elif line.startswith('#EXTVLCOPT:'):
				opts = line[11:].split('=', 1)
				if len(opts) == 2:
					key = opts[0].lower().strip()
					value = opts[1].strip()
					if key == 'http-user-agent':
						current_params['user_agent'] = value
					elif key == 'program':
						current_params['program-id'] = value

			elif line.startswith('#'):
				continue

			else:
				if current_params.get('title'):
					current_params['uri'] = line.strip()
					entries.append(current_params)
					current_params = {}

		return entries

	def process_url(self, url):
		"""Process URLs based on settings"""
		url = url.replace(":", "%3a")
		if config.plugins.m3uconverter.hls_convert.value:
			if any(url.lower().endswith(x) for x in ('.m3u8', '.stream')):
				url = f"hls://{url}"
		return url

	def convert_hls(self, url):
		"""Convert HLS URLs"""
		parsed = urlparse(url)
		if parsed.path.lower().endswith(('.m3u8', '.stream')):
			return f"hls://{url}"
		return url

	def update_main_bouquet(self, groups):
		"""Update the main bouquet file with generated group bouquets"""
		main_file = "/etc/enigma2/bouquets.tv"
		existing = []

		if exists(main_file):
			with open(main_file, "r", encoding="utf-8") as f:
				existing = f.readlines()

		if config.plugins.m3uconverter.backup_enable.value:
			create_backup()

		new_lines = []
		for group in groups:
			safe_name = self.get_safe_filename(group)
			bouquet_path = "userbouquet." + safe_name + ".tv"
			line_to_add = '#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "' + bouquet_path + '" ORDER BY bouquet\n'

			if line_to_add not in existing and line_to_add not in new_lines:
				new_lines.append(line_to_add)

		# Decide if add at top or bottom based on config option
		if config.plugins.m3uconverter.bouquet_position.value == "top":
			new_content = new_lines + existing
		else:
			new_content = existing + new_lines

		with open(main_file, "w", encoding="utf-8") as f:
			f.writelines(new_content)

		if config.plugins.m3uconverter.auto_reload.value:
			reload_services()

	def write_group_bouquet(self, group, channels):
		"""
		Writes a bouquet file for a single group safely and efficiently,
		handling cirillic characters by transliterating names and saving
		the file in latin-1 encoding for Enigma2 compatibility.
		"""
		try:
			safe_name = self.get_safe_filename(group)
			filename = join("/etc/enigma2", "userbouquet." + safe_name + ".tv")
			temp_file = filename + ".tmp"

			# Transliterate group name and provide fallback
			name_bouquet = transliterate(group).capitalize()
			if not name_bouquet.strip():
				name_bouquet = "Unnamed Group"

			# Add marker and main bouquet name
			markera = "#SERVICE 1:64:0:0:0:0:0:0:0:0::--- | M3UConverter | ---"
			markerb = "#DESCRIPTION --- | M3UConverter | ---"

			with open(temp_file, "w", encoding="latin-1", errors="replace") as f:
				f.write("#NAME " + name_bouquet + "\n")
				f.write(markera + "\n")
				f.write(markerb + "\n")
				for idx, ch in enumerate(channels, 1):
					f.write("#SERVICE 4097:0:1:0:0:0:0:0:0:0:" + ch["url"] + "\n")
					desc = transliterate(ch["name"])
					f.write("#DESCRIPTION " + desc + "\n")

					if idx % 50 == 0:
						f.flush()
						fsync(f.fileno())

			replace(temp_file, filename)
			chmod(filename, 0o644)

		except Exception as e:
			if exists(temp_file):
				try:
					remove(temp_file)
				except Exception as cleanup_error:
					logger.error("Cleanup error: " + str(cleanup_error))

			logger.error("Failed to write bouquet " + group + ": " + str(e))
			raise RuntimeError("Bouquet creation failed for " + group) from e

	def get_safe_filename(self, group):
		"""Generate a secure file name for bouquets with a suffix for identification"""
		normalized = unicodedata.normalize("NFKD", group)
		safe_name = normalized.encode('ascii', 'ignore').decode('ascii')  # Remove accents and non-ASCII
		safe_name = sub(r'[^a-z0-9_-]', '_', safe_name.lower())           # Replace unwanted chars
		safe_name = sub(r'_+', '_', safe_name).strip('_')                 # Normalize underscores

		# Add a suffix to mark the bouquet as created by M3UConverter
		suffix = "_m3ubouquet"
		base_name = safe_name[:50 - len(suffix)] if len(safe_name) > 50 - len(suffix) else safe_name

		return base_name + suffix if base_name else "my_m3uconverter" + suffix

	# TV to M3U Conversion Methods
	def parse_tv(self, filename):
		try:
			channels = []
			with codecs.open(filename, "r", encoding="utf-8") as f:
				content = f.read()
				matches = findall(
					r'#SERVICE\s(?:4097|5002):\d+:\d+:\d+:\d+:\d+:\d+:\d+:\d+:\d+:(.*?)\n#DESCRIPTION\s(.*?)\n',
					content,
					DOTALL
				)

				for service, name in matches:
					# URL decoding and filtering HTTP/HTTPS streams only
					url = unquote(service.strip())
					if any(url.startswith(proto) for proto in ('http://', 'https://', 'hls://')):
						channels.append((name.strip(), url))

			if not channels:
				raise ValueError(_("No IPTV channels found in the bouquet"))

			self.m3u_list = channels
			self["list"].setList([c[0] for c in channels])
			self["status"].setText(_("Loaded %d channels") % len(channels))

		except Exception as e:
			self.show_error(_("Error parsing bouquet: %s") % str(e))

	def convert_tv_to_m3u(self):

		def conversion_task():
			try:
				output_file = "/tmp/converted.m3u"
				with open(output_file, 'w', encoding='utf-8') as f:
					f.write('#EXTM3U\n')
					for idx, (name, url) in enumerate(self.m3u_list):
						f.write(f'#EXTINF:-1 tvg-name="{quote(name)}",{name}\n')
						f.write(f'{url}\n')
						self.update_progress(idx + 1, _("Exporting: %s") % name)
				return (True, output_file)
			except Exception as e:
				return (False, str(e))

		threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

	def conversion_finished(self, result):
		self["progress_source"].setValue(0)
		success, data = result

		if success:
			msg = _("Successfully converted %d items") % data if isinstance(data, int) else _("File saved to: %s") % data
			self.show_info(msg)
			self["status"].setText(msg)
			self["progress_source"].setValue(self["progress_source"].range)
		else:
			msg = _("Conversion failed: %s") % data
			self.show_error(msg)
			self["progress_source"].setValue(0)
			self["status"].setText(msg)

		self["progress_text"].setText("")

	def update_progress(self, value, text):
		from twisted.internet import reactor
		reactor.callFromThread(self._update_progress_ui, value, text)

	def _update_progress_ui(self, value, text):
		self.progress_source.setRange(len(self.m3u_list) if self.m3u_list else 100)
		self.progress_source.setValue(value)
		self["progress_text"].setText(text)

	def open_settings(self):
		self.session.open(M3UConverterSettings)

	def show_credits(self):
		text = f"M3U Archimede Universal Converter Plugin\nVersion {str(currversion)}\nLululla Developed for Enigma2"
		self.session.open(MessageBox, text, MessageBox.TYPE_INFO)

	def show_info(self, message):
		self.session.open(MessageBox, message, MessageBox.TYPE_INFO)
		self["status"].setText(message)

	def show_error(self, message):
		self.session.open(MessageBox, message, MessageBox.TYPE_ERROR)
		self["status"].setText(message)


class M3UConverterSettings(Setup):
	def __init__(self, session, parent=None):
		Setup.__init__(self, session, setup="M3UConverterSettings", plugin="Extensions/M3UConverter")
		self.parent = parent

	def keySave(self):
		Setup.keySave(self)


def main(session, **kwargs):
	session.openWithCallback(
		lambda conversion_type: conversion_type and session.openWithCallback(
			lambda: None,  # Callback vuoto per mantenere la modalità
			UniversalConverter,
			conversion_type
		),
		ConversionSelector
	)


def Plugins(**kwargs):
	from Plugins.Plugin import PluginDescriptor
	return [PluginDescriptor(
		name=_("Universal Converter"),
		description=_("Convert between M3U and Enigma2 bouquets"),
		where=PluginDescriptor.WHERE_PLUGINMENU,
		icon="plugin.png",
		fnc=main)
	]
