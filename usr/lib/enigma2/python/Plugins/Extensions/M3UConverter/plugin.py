# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 1.6                                         #
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

import codecs
import unicodedata
from time import strftime
from gettext import gettext as _
from os import access, listdir, makedirs, remove, replace, chmod, fsync, W_OK
from os.path import dirname, exists, isdir, isfile, join, normpath
from re import sub, findall, DOTALL, search
from urllib.parse import quote, urlparse, unquote

from twisted.internet import threads
from enigma import eDVBDB, eServiceReference, getDesktop

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

from Tools.Directories import fileExists, resolveFilename
from Tools.Directories import defaultRecordingLocation, SCOPE_MEDIA


try:
	from Components.AVSwitch import AVSwitch
except ImportError:
	from Components.AVSwitch import eAVControl as AVSwitch


class AspectManager:
	def __init__(self):
		self.init_aspect = self.get_current_aspect()
		print("[INFO] Initial aspect ratio:", self.init_aspect)

	def get_current_aspect(self):
		"""Restituisce l'aspect ratio attuale del dispositivo."""
		try:
			return int(AVSwitch().getAspectRatioSetting())
		except Exception as e:
			print("[ERROR] Failed to get aspect ratio:", str(e))
			return 0

	def restore_aspect(self):
		"""Ripristina l'aspect ratio originale all'uscita del plugin."""
		try:
			print("[INFO] Restoring aspect ratio to:", self.init_aspect)
			AVSwitch().setAspectRatio(self.init_aspect)
		except Exception as e:
			print("[ERROR] Failed to restore aspect ratio:", str(e))


# for my friend Archimede
currversion = '1.6'
title_plug = _("Archimede Universal Converter v.%s by Lululla") % currversion
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2

aspect_manager = AspectManager()
screenwidth = getDesktop(0).size()
screen_width = screenwidth.width()
# logger = logging.getLogger("UniversalConverter")


# add lululla for debug
class ColoredLogger:
	LEVELS = {
		"DEBUG": ("\033[92m", "[DEBUG]"),  # verde
		"INFO": ("\033[97m", "[INFO] "),  # bianco
		"WARNING": ("\033[93m", "[WARN] "),  # giallo
		"ERROR": ("\033[91m", "[ERROR]"),  # rosso
	}
	END = "\033[0m"

	def __init__(self, log_file=None, clear_on_start=True):
		self.log_file = log_file
		if self.log_file and clear_on_start:
			try:
				remove(self.log_file)
			except Exception:
				pass  # silently ignore if file doesn't exist

	def log(self, level, message):
		prefix, label = self.LEVELS.get(level, ("", "[LOG] "))
		timestamp = strftime("%Y-%m-%d %H:%M:%S")
		formatted = "%s %s %s%s%s" % (timestamp, label, prefix, message, self.END)

		# Print to console
		print(formatted)

		# Also write to file if enabled
		if self.log_file:
			try:
				with open(self.log_file, "a") as f:
					f.write("%s %s %s\n" % (timestamp, label, message))
			except Exception:
				pass  # never crash the app


logger = ColoredLogger(log_file="/tmp/m3u_converter.log")
logger.log("INFO", f"START PLUGIN {str(title_plug)}")


# Ensure movie path
def defaultMoviePath():
	result = config.usage.default_path.value
	if not result.endswith("/"):
		result += "/"
	if not isdir(result):
		return defaultRecordingLocation(config.usage.default_path.value)
	return result


def get_mounted_devices():
	"""Recovers mounted devices with write permissions"""
	from Components.Harddisk import harddiskmanager

	devices = [
		(resolveFilename(SCOPE_MEDIA, "hdd"), _("Hard Disk")),
		(resolveFilename(SCOPE_MEDIA, "usb"), _("USB Drive"))
	]

	devices.append(("/tmp/", _("Temporary Storage")))

	try:
		devices += [
			(p.mountpoint, p.description or _("Disk"))
			for p in harddiskmanager.getMountedPartitions()
			if p.mountpoint and access(p.mountpoint, W_OK)
		]

		net_dir = resolveFilename(SCOPE_MEDIA, "net")
		if isdir(net_dir):
			devices += [(join(net_dir, d), _("Network")) for d in listdir(net_dir)]

	except Exception as e:
		logger.log("ERROR", "Mount error: %s" % str(e))

	unique_devices = {}
	for p, d in devices:
		path = p.rstrip("/") + "/"
		if isdir(path):
			unique_devices[path] = d

	return list(unique_devices.items())


def update_mounts():
	"""Update the list of mounted devices and update config choices"""
	mounts = get_mounted_devices()
	if not mounts:
		default_path = defaultMoviePath()
		mounts = [(default_path, default_path)]
	config.plugins.m3uconverter.lastdir.setChoices(mounts, default=mounts[0][0])
	config.plugins.m3uconverter.lastdir.save()


# Init Config
config.plugins.m3uconverter = ConfigSubsection()
default_dir = config.movielist.last_videodir.value if isdir(config.movielist.last_videodir.value) else defaultMoviePath()
config.plugins.m3uconverter.lastdir = ConfigSelection(default=default_dir, choices=[])
config.plugins.m3uconverter.bouquet_position = ConfigSelection(
	default="bottom",
	choices=[("top", _("Top")), ("bottom", _("Bottom"))]
)
config.plugins.m3uconverter.hls_convert = ConfigYesNo(default=True)
config.plugins.m3uconverter.auto_reload = ConfigYesNo(default=True)
config.plugins.m3uconverter.backup_enable = ConfigYesNo(default=True)
update_mounts()


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


class M3UFileBrowser(Screen):
	def __init__(self, session, startdir="/etc/enigma2", matchingPattern=r"(?i)^.*\.tv$", conversion_type=None):
		Screen.__init__(self, session)
		self.skinName = "FileBrowser"
		self.conversion_type = conversion_type
		self["filelist"] = FileList(
			startdir,
			matchingPattern=matchingPattern,
			showDirectories=True,
			showFiles=True,
			useServiceRef=False
		)

		self["actions"] = ActionMap(["OkCancelActions", "ColorActions"], {
			"ok": self.ok_pressed,
			"green": self.ok_pressed,
			"cancel": self.close
		}, -1)
		if self.conversion_type == "tv_to_m3u":
			self.onShown.append(self._filter_list)  # Filter after screen is shown

	def _filter_list(self):
		"""Filter list to show only directories and .tv files containing 'http'"""
		filtered = []
		for entry in self["filelist"].list:
			if not entry or not isinstance(entry[0], tuple):
				continue  # Skip invalid entries

			file_data = entry[0]
			path = None
			is_dir = False

			# Handle different tuple formats
			if len(file_data) >= 2:
				# Standard format: (path, is_dir, ...)
				path = file_data[0]
				is_dir = file_data[1]

			elif len(file_data) == 1 and isinstance(file_data[0], str):
				# Special case for parent directory: ('..',)
				path = file_data[0]
				is_dir = True
			else:
				logger.log("DEBUG", f"Skipping invalid entry: {file_data}")
				continue

			# Handle special cases like parent directory
			if path == ".." or is_dir:
				filtered.append(entry)
			else:
				# Only include .tv files with HTTP content
				if path and path.lower().endswith(".tv") and self._contains_http(path):
					filtered.append(entry)

		self["filelist"].list = filtered
		self["filelist"].l.setList(filtered)

	def _contains_http(self, filename):
		"""Check if file contains 'http' (case-insensitive) with full path"""
		try:
			# Get the current directory to form the full path
			current_dir = self["filelist"].getCurrentDirectory()
			full_path = join(current_dir, filename)

			with open(full_path, "r") as f:
				return any("http" in line.lower() for line in f)
		except Exception as e:
			logger.log("ERROR", f"Error reading {full_path}: {str(e)}")
			return False

	def ok_pressed(self):
		selection = self["filelist"].getCurrent()
		if not selection or not isinstance(selection, list) or not isinstance(selection[0], tuple):
			logger.log("ERROR", f"Invalid selection format: {selection}")
			return

		file_data = selection[0]
		path = file_data[0]
		is_dir = file_data[1]
		dir_icon = None
		logger.log("INFO", f"file_data: {file_data}, path: {path}, is_dir: {is_dir}")
		try:
			if dir_icon in (ICON_STORAGE, ICON_PARENT, ICON_CURRENT):
				self["filelist"].changeDir(path)
				if self.conversion_type == "tv_to_m3u":
					self._filter_list()
			elif is_dir:
				self["filelist"].changeDir(path)
				if self.conversion_type == "tv_to_m3u":
					self._filter_list()
			else:
				current_dir = self["filelist"].getCurrentDirectory()
				full_path = join(current_dir, path)
				logger.log("INFO", f"Selected full file path: {full_path}")
				self.close(full_path)
		except Exception as e:
			logger.log("ERROR", f"ok_pressed error: {str(e)}")


class ConversionSelector(Screen):

	skin = """
	<screen name="ConversionSelector" position="center,center" size="1280,720" title="Archimede Universal Converter - Select Type" flags="wfNoBorder">
		<eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
		<widget name="list" position="20,20" size="1240,559" itemHeight="50" font="Regular;34" scrollbarMode="showNever" />
		<widget name="status" position="20,605" size="1240,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
		<eLabel backgroundColor="red" cornerRadius="3" position="50,700" size="300,6" zPosition="11" />
		<eLabel backgroundColor="green" cornerRadius="3" position="347,700" size="300,6" zPosition="11" />
		<eLabel backgroundColor="yellow" cornerRadius="3" position="647,700" size="300,6" zPosition="11" />
		<!--
		<eLabel backgroundColor="blue" cornerRadius="3" position="882,720" size="200,6" zPosition="11" />
		-->
		<widget source="key_red" render="Label" position="50,660" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
		<widget source="key_green" render="Label" position="350,660" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
		<widget source="key_yellow" render="Label" position="650,660" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
		<!--
		<widget source="key_blue" render="Label" position="880,685" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white"/>
		-->
	</screen>"""

	def __init__(self, session):
		Screen.__init__(self, session)
		self.session = session
		self.skinName = "ConversionSelector"
		self.is_modal = True
		self.setTitle(title_plug)
		a = _("Convert M3U to Enigma2 Bouquets")
		b = _("Convert Enigma2 Bouquets to M3U")
		c = _("Remove Enigma2 Bouquets Created with M3UConverter")

		self.menu = [
			(a, "m3u_to_tv"),
			(b, "tv_to_m3u"),
			(c, "purge_m3u_bouquets"),
		]
		self["list"] = MenuList(self.menu)
		self["status"] = Label(_("We're ready: what do you want to do?"))
		self["actions"] = ActionMap(["ColorActions", "OkCancelActions"], {
			# "red": lambda: self.close(None),
			"red": self.close,
			"green": self.select_item,
			"ok": self.select_item,
			"yellow": self.purge_m3u_bouquets,
			# "cancel": lambda: self.close(None)
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
					logger.log("ERROR", f"Failed to remove: {str(f)} Error {str(e)}")

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
	if screen_width > 1280:

		skin = """
		<screen position="-25,-20" size="1920,1080" title="Archimede Universal Converter" flags="wfNoBorder">
			<widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
			<eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1920,1080" zPosition="-2" />
			<widget name="list" position="65,70" size="1122,797" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
			<widget name="status" position="65,920" size="1127,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
			<eLabel backgroundColor="red" cornerRadius="3" position="65,1025" size="300,6" zPosition="11" />
			<eLabel backgroundColor="green" cornerRadius="3" position="366,1025" size="300,6" zPosition="11" />
			<eLabel backgroundColor="yellow" cornerRadius="3" position="666,1025" size="300,6" zPosition="11" />
			<eLabel backgroundColor="blue" cornerRadius="3" position="966,1025" size="300,6" zPosition="11" />
			<widget source="key_red" render="Label" position="65,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="key_green" render="Label" position="365,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="key_yellow" render="Label" position="664,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="key_blue" render="Label" position="967,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="progress_source" render="Progress" position="65,880" size="1125,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
			<widget source="progress_text" render="Label" position="65,880" size="1124,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
			<eLabel name="" position="1200,875" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="28" font="Regular; 17" zPosition="1" text="OK" />
			<eLabel name="" position="1200,930" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="28" font="Regular; 17" zPosition="1" text="STOP" />
			<widget source="session.CurrentService" render="Label" position="1220,125" size="640,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
				<convert type="ServiceName">Name</convert>
			</widget>
			<widget source="session.VideoPicture" render="Pig" position="1220,166" zPosition="20" size="640,360" backgroundColor="transparent" transparent="0" cornerRadius="14" />
		</screen>"""

	else:
		skin = """
		<screen position="center,center" size="1280,720" title="Archimede Universal Converter" flags="wfNoBorder">
			<widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
			<eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
			<widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
			<widget name="status" position="23,608" size="1185,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
			<eLabel backgroundColor="red" cornerRadius="3" position="30,700" size="275,6" zPosition="11" />
			<eLabel backgroundColor="green" cornerRadius="3" position="305,700" size="275,6" zPosition="11" />
			<eLabel backgroundColor="yellow" cornerRadius="3" position="580,700" size="275,6" zPosition="11" />
			<eLabel backgroundColor="blue" cornerRadius="3" position="855,700" size="275,6" zPosition="11" />
			<widget source="key_red" render="Label" position="30,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="key_green" render="Label" position="305,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="key_yellow" render="Label" position="580,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="key_blue" render="Label" position="855,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
			<widget source="progress_source" render="Progress" position="25,582" size="1180,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
			<widget source="progress_text" render="Label" position="24,582" size="1180,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
			<eLabel name="" position="1141,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="28" font="Regular; 17" zPosition="1" text="OK" />
			<eLabel name="" position="1195,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="28" font="Regular; 17" zPosition="1" text="STOP" />
			<widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
				<convert type="ServiceName">Name</convert>
			</widget>
			<widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
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
		self["Title"] = Label(title_plug)
		self["status"] = Label(_("Ready"))
		self["key_red"] = StaticText(_("Select"))
		self["key_green"] = StaticText("")
		self["key_yellow"] = StaticText(_("Settings"))
		self["key_blue"] = StaticText(_("Info"))
		self["progress_source"] = self.progress_source
		self["progress_text"] = StaticText("")
		self["progress_source"].setValue(0)
		self.setTitle(title_plug)
		self.initialservice = self.session.nav.getCurrentlyPlayingServiceReference()
		self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MediaPlayerActions"], {
			"red": self.open_file,
			"green": self.start_conversion,
			"yellow": self.open_settings,
			"blue": self.show_credits,
			"cancel": self.keyClose,
			"stop": self.stop_player,
			"ok": self.key_ok
		}, -1)

		if self.conversion_type == "m3u_to_tv":
			# self.init_m3u_converter()
			pass
		else:
			self.conversion_type = "tv_to_m3u"
			self.init_tv_converter()

		self["status"] = Label(_("Ready: Select the file from the %s you configured in settings.") % self.full_path)

	def init_m3u_converter(self):
		self.onShown.append(self.delayed_file_browser)

	def delayed_file_browser(self):
		try:
			self.onShown.remove(self.delayed_file_browser)
		except ValueError:
			pass
		# from twisted.internet import reactor
		# reactor.callLater(0.2, self.open_file_browser)

	def init_tv_converter(self):
		self.update_path_tv()

	def update_path_tv(self):
		try:
			if not exists("/etc/enigma2"):
				raise OSError("Bouquets path not found")

			if not access("/etc/enigma2", W_OK):
				logger.log("WARNING", "Read-only bouquets path /etc/enigma2")

		except Exception as e:
			logger.log("ERROR", f"TV path error: {str(e)}")
			self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR)

	def open_file_browser(self):
		path = config.plugins.m3uconverter.lastdir.value
		if not path or not isdir(path):
			path = "/media/hdd" if isdir("/media/hdd") else "/tmp"

		pattern = r"(?i)^.*\.tv$" if self.conversion_type != "m3u_to_tv" else r"(?i)^.*\.m3u$"
		try:
			self.session.openWithCallback(
				self.file_selected,
				M3UFileBrowser,
				path,
				matchingPattern=pattern,
				conversion_type=self.conversion_type
			)
		except Exception as e:
			logger.log("ERROR", f"Error opening file browser: {str(e)}")
			self.session.open(MessageBox, _("Unable to open file browser."), MessageBox.TYPE_ERROR)

	def start_conversion(self):
		if self.conversion_type == "m3u_to_tv":
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

	def update_path(self):
		"""Update path with special handling for /tmp and ensure it's a valid directory."""
		try:
			if self.conversion_type == "tv_to_m3u":
				self.full_path = "/etc/enigma2"
				if not isdir(self.full_path):
					logger.log("WARNING", f"Path {self.full_path} does not exist, falling back to /tmp")
					self.full_path = "/tmp"
			else:
				base_path = config.plugins.m3uconverter.lastdir.value
				"""
				if not isinstance(base_path, (str, bytes)):
					logger.log("WARNING", f"Invalid base_path type: {type(base_path)}, resetting to empty string")
					base_path = ""
				"""
				fallbacks = ["/media/hdd", "/media/usb", "/tmp"]
				if not base_path or not isdir(base_path):
					base_path = next((p for p in fallbacks if isdir(p)), "/tmp")

				if base_path == "/tmp":
					self.full_path = base_path
				else:
					self.full_path = join(base_path, "movie")
					if not isdir(self.full_path):
						makedirs(self.full_path, exist_ok=True)
						chmod(self.full_path, 0o755)
			"""
			if not isinstance(self.full_path, (str, bytes)):
				logger.log("ERROR", f"Invalid path type: {type(self.full_path)}, expected str or bytes. Falling back to /tmp")
				self.full_path = "/tmp"
			"""
			if not isdir(self.full_path):
				logger.log("ERROR", f"Final path {self.full_path} is not a directory, falling back to /tmp")
				self.full_path = "/tmp"

			logger.log("INFO", f"Using path: {self.full_path}")

		except Exception as e:
			logger.log("ERROR", f"Path update failed: {str(e)}")
			self.full_path = "/tmp"

	def open_file(self):
		logger.log("DEBUG", f"Path self.conversion_type: {self.conversion_type}")

		if self.conversion_type == "m3u_to_tv":
			self.open_file_browser()
		else:
			try:
				self.update_path()
				logger.log("DEBUG", "Conversion type: %s, Full path: %s" % (self.conversion_type, self.full_path))
				pattern = r"(?i)^.*\.tv$" if self.conversion_type == "tv_to_m3u" else r"(?i)^.*\.m3u$"
				logger.log("DEBUG", "Using pattern: %s" % pattern)
				self.session.openWithCallback(
					self.file_selected,
					M3UFileBrowser,
					self.full_path,
					matchingPattern=pattern,
					conversion_type=self.conversion_type
				)
			except Exception as e:
				logger.log("ERROR", "Browser error: %s" % str(e))
				self.session.open(
					MessageBox,
					_("Error browser file:\n%s") % str(e),
					MessageBox.TYPE_ERROR
				)

	def file_selected(self, res=None):
		logger.log("INFO", f"Callback file_selected: {str(res)}")

		if not res:
			self["status"].setText(_("No files selected"))
			return

		# Directly use the full path
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

			# Validate extension
			if self.conversion_type == "m3u_to_tv":
				if not selected_path.lower().endswith(".m3u"):
					raise ValueError(_("Select a valid M3U file"))
			else:
				if not selected_path.lower().endswith(".tv"):
					raise ValueError(_("Select a valid TV bouquet"))

			# Save current directory
			config_dir = dirname(selected_path)
			if isdir(config_dir):
				config.plugins.m3uconverter.lastdir.value = config_dir
				config.plugins.m3uconverter.lastdir.save()

			self.selected_file = selected_path

			# Parse selected file
			if self.conversion_type == "m3u_to_tv":
				self.parse_m3u(selected_path)
			else:
				self.parse_tv(selected_path)

		except Exception as e:
			logger.log("ERROR", f"Error file selected: {str(e)}", exc_info=True)
			self["status"].setText(_("Error: %s") % str(e))
			self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR)

	def parse_m3u(self, filename):
		"""Analyze M3U files with advanced attribute management"""
		try:
			logger.log("INFO", f"Parsing M3U: {filename}")
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
			self["status"].setText(_("Loaded %d channels. Are you ready to convert? Press Green to proceed.") % len(self.m3u_list))
			self["key_green"].setText(_("Convert to M3U"))

		except Exception as e:
			logger.log("ERROR", f"Error Parsing M3U: {str(e)}")
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
				match = search(r"([^/\\]+)\.m3u$", self.selected_file, flags=DOTALL)
				if match:
					name_bouquet = transliterate(match.group(1)).capitalize()
				else:
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
					logger.log("ERROR", f"Cleanup error: {str(cleanup_error)}")

			logger.log("ERROR", f"Failed to write bouquet {str(group)} : {str(e)}")
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
				# More robust pattern that handles different spacing and optional fields
				pattern = r'#SERVICE\s(?:4097|5002):[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:(.*?)\s*\n#DESCRIPTION\s*(.*?)\s*\n'
				matches = findall(pattern, content, DOTALL)

				for service, name in matches:
					# URL decoding and filtering HTTP/HTTPS/HLS streams
					url = unquote(service.strip())
					if any(url.startswith(proto) for proto in ('http://', 'https://', 'hls://')):
						channels.append((name.strip(), url))

			if not channels:
				raise ValueError(_("No IPTV channels found in the bouquet"))

			self.m3u_list = channels
			self["list"].setList([c[0] for c in channels])
			self["status"].setText(_("Loaded %d channels. Are you ready to convert? Press Green to proceed.") % len(channels))
			self["key_green"].setText(_("Convert to Bouquet"))
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
		msg = ''
		if success:
			msg = _("Successfully converted %d items") % data if isinstance(data, int) else _("File saved to: %s") % data
			self.show_info(msg)
			self["progress_source"].setValue(0)
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

	def key_ok(self):
		index = self["list"].getSelectedIndex()
		if index < 0 or index >= len(self.m3u_list):
			self["status"].setText(_("No item selected"))
			return

		item = self.m3u_list[index]

		if self.conversion_type == "tv_to_m3u":
			# If loaded from .tv (tuple)
			if isinstance(item, tuple) and len(item) == 2:
				name, url = item
				url = unquote(url.strip())
			else:
				self["status"].setText(_("Invalid .tv item format"))
				return
		else:
			# Default: assume it's from .m3u (dict)
			if isinstance(item, dict):
				name = item.get("name", "")
				url = item.get("url", "")
				url = unquote(url.strip())
			else:
				self["status"].setText(_("Invalid .m3u item format"))
				return

		self["status"].setText(_("Playing: %s") % name)
		self.start_player(name, url)

	def start_player(self, name, url):
		stream = eServiceReference(4097, 0, url)
		stream.setName(name)
		logger.log("DEBUG", f"Extracted video URL: {str(stream)}")
		self.session.nav.stopService()
		self.session.nav.playService(stream)

	def stop_player(self):
		self.session.nav.stopService()
		self.session.nav.playService(self.initialservice)
		self["status"].setText(_("Ready"))

	def on_movieplayer_exit(self, result=None):
		self.session.nav.stopService()
		self.session.nav.playService(self.initialservice)
		aspect_manager.restore_aspect()
		self.close()

	def keyClose(self, result=None):
		try:
			self.session.nav.stopService()
			self.session.nav.playService(self.initialservice)
			aspect_manager.restore_aspect()
			self.close()
		except Exception as e:
			print('error:', str(e))

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
	def on_conversion_selected(result=None):
		if result:
			session.open(UniversalConverter, conversion_type=result)

	session.openWithCallback(on_conversion_selected, ConversionSelector)


def Plugins(**kwargs):
	from Plugins.Plugin import PluginDescriptor
	return [PluginDescriptor(
		name=_("Universal Converter"),
		description=_("Convert between M3U and Enigma2 bouquets"),
		where=PluginDescriptor.WHERE_PLUGINMENU,
		icon="plugin.png",
		fnc=main)
	]
