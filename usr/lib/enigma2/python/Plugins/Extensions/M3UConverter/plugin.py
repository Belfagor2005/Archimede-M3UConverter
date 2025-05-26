# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede M3uConverter Plugin                        #
#  Version: 1.0                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "11:56 - 20250526"                    #
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
from gettext import gettext as _
from os import access, listdir, makedirs, remove, replace, chmod, fsync, W_OK
from os.path import basename, dirname, exists, isdir, join, normpath, splitext
from re import sub
from urllib.parse import quote, urlparse
from twisted.internet import threads
from Components.ActionMap import ActionMap
from Components.ConfigList import ConfigListScreen
from Components.FileList import FileList
from Components.Label import Label
from Components.MenuList import MenuList
from Components.Sources.Progress import Progress
from Components.Sources.StaticText import StaticText
from Components.config import config, ConfigSelection, ConfigSubsection, ConfigYesNo
from Screens.MessageBox import MessageBox
from Screens.Screen import Screen
from Tools.Directories import fileExists, resolveFilename, SCOPE_MEDIA

# for my friend Archimede
currversion = '1.0'

logger = logging.getLogger("M3UConverter")

# Configurazione iniziale
config.plugins.m3uconverter = ConfigSubsection()
config.plugins.m3uconverter.lastdir = ConfigSelection(default="/media/hdd", choices=[])
config.plugins.m3uconverter.hls_convert = ConfigYesNo(default=True)
config.plugins.m3uconverter.auto_reload = ConfigYesNo(default=True)
config.plugins.m3uconverter.backup_enable = ConfigYesNo(default=True)


class M3UFileBrowser(Screen):
    def __init__(self, session, startdir="/media/hdd"):
        Screen.__init__(self, session)
        self.skinName = "FileBrowser"

        self["title"] = Label("Select M3U file")
        self["filelist"] = FileList(
            startdir,
            matchingPattern=r"(?i)^.*\.m3u$",
            showDirectories=True,
            showFiles=True,
            useServiceRef=False
        )

        self["actions"] = ActionMap(["OkCancelActions", "ColorActions"], {
            "ok": self.ok_pressed,
            "OK": self.ok_pressed,
            "green": self.ok_pressed,
            "cancel": self.close
        }, -1)

    def ok_pressed(self):
        print("OK pressed")
        selection = self["filelist"].getCurrent()
        print("Selection:", selection)
        if selection:
            self.close(selection[0])


class M3UConverter(Screen):
    skin = """
    <screen position="center,center" size="1280,720" title="Archimede M3U Converter">
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

    def __init__(self, session):
        Screen.__init__(self, session)
        self.session = session
        self.m3u_list = []
        self.selected_file = ""
        self.progress = None

        self["list"] = MenuList([])
        self["status"] = Label("No files selected")
        self["key_red"] = StaticText(_("Open"))
        self["key_green"] = StaticText(_("Export"))
        self["key_yellow"] = StaticText(_("Settings"))
        self["key_blue"] = StaticText(_("Info"))
        self.progress_source = Progress()
        self["progress_source"] = self.progress_source
        self["progress_text"] = StaticText()
        self["actions"] = ActionMap(["OkCancelActions", "ColorActions"], {
            "red": self.open_file,
            "green": self.start_export,
            "yellow": self.open_settings,
            "blue": self.show_info,
            "cancel": self.exit
        }, -1)

        self.update_mounts()
        self.update_path()

    def update_mounts(self):
        """Aggiorna la lista dei dispositivi montati"""
        mounts = self.get_mounted_devices()
        config.plugins.m3uconverter.lastdir.setChoices(mounts)
        config.plugins.m3uconverter.lastdir.save()

    def get_mounted_devices(self):
        """Recupera i dispositivi montati con permessi di scrittura"""
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
        """Update path with special handling for /tmp"""
        try:
            base_path = config.plugins.m3uconverter.lastdir.value

            # Verifica e fallback per il percorso base
            if not base_path or not isdir(base_path):
                fallbacks = ["/media/hdd", "/media/usb", "/tmp"]
                base_path = next((p for p in fallbacks if isdir(p)), "/tmp")

            # Gestione speciale per /tmp
            if base_path == "/tmp":
                self.full_path = base_path
            else:
                self.full_path = join(base_path, "movie")

            if base_path != "/tmp":
                makedirs(self.full_path, exist_ok=True)
                chmod(self.full_path, 0o755)

        except Exception as e:
            logger.error(f"Path update failed: {str(e)}")
            self.full_path = "/tmp"

    def open_file(self):
        """Opens the selector file without selectedItems parameter"""
        try:
            self.session.openWithCallback(
                self.file_selected,
                M3UFileBrowser,
                self.full_path
            )
        except Exception as e:
            logger.error(f"Browser error: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error browser file:\n%s") % str(e),
                MessageBox.TYPE_ERROR
            )

    def file_selected(self, res):
        logger.info("Callback file_selected: %s" % res)
        if res and fileExists(res[0]):
            try:
                selected_path = normpath(res[0])

                if not selected_path.lower().endswith('.m3u'):
                    raise ValueError("Unsupported file format")

                config_dir = dirname(selected_path)
                logger.info("Callback config_dir: %s" % config_dir)
                if isdir(config_dir):
                    config.plugins.m3uconverter.lastdir.value = config_dir
                    config.plugins.m3uconverter.lastdir.save()
                self.selected_file = selected_path
                self.parse_m3u(selected_path)
            except Exception as e:
                logger.error("File selection error: %s" % str(e))
                self["status"].setText(_("Invalid selection"))
        else:
            self["status"].setText(_("No files selected"))

    def parse_m3u(self, filename):
        """Analyze M3U files with advanced attribute management"""
        try:
            with open(filename, 'r', encoding='utf-8', errors='replace') as f:
                data = f.read()

            self.m3u_list = []
            self.filename = filename
            channels = self._parse_m3u_content(data)
            # Mappatura attributi avanzati
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

            # Aggiornamento UI
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
            self["status"].setText(_("Caricati %d canali") % len(self.m3u_list))

        except Exception as e:
            logger.error(f"Errore parsing M3U: {str(e)}")
            self.session.open(
                MessageBox,
                _("Formato file non valido:\n%s") % str(e),
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

            else:  # URL del canale
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

    def start_export(self):
        self.progress_source.setRange(len(self.m3u_list))
        self.progress_source.setValue(0)
        self["progress_text"].setText(_("Starting export..."))

        def export_task():
            try:
                self.name_file = self.get_safe_filename(splitext(basename(self.filename))[0])
                # 1. Backup iniziale
                if config.plugins.m3uconverter.backup_enable.value:
                    self.create_backup()

                # 2. Elaborazione canali
                groups = {}
                for idx, channel in enumerate(self.m3u_list):
                    groups.setdefault(channel['group'], []).append(channel)
                    self.update_progress(idx + 1, _("Processing %s") % channel['name'])

                # 3. Scrittura DOPO l'elaborazione completa
                self.update_main_bouquet(groups.keys())

                for group, channels in groups.items():
                    self.write_group_bouquet(group, channels)

                # 4. Ricarica finale
                if config.plugins.m3uconverter.auto_reload.value:
                    self.reload_services()

                return (True, len(self.m3u_list))
            except Exception as e:
                import traceback
                tb = traceback.format_exc()  # Ottieni stack trace completo
                self.update_progress(0, _("Error: %s") % str(e))
                logger.error("Export failed:\n%s", tb)
                return (False, str(e))

        threads.deferToThread(export_task).addBoth(self.export_finished)

    def export_finished(self, result):
        success, data = result
        if success:
            msg = _("Exported %d channels") % data
            self.progress_source.setValue(self.progress_source.range)
        else:
            msg = _("Export failed: %s") % data
            self.progress_source.setValue(0)

        self["progress_text"].setText(msg)

    def update_progress(self, value, text):
        from twisted.internet import reactor
        reactor.callFromThread(self._update_ui, value, text)

    def _update_ui(self, value, text):
        self.progress_source.setValue(value)
        self["progress_text"].setText(text)

    def create_backup(self):
        """Create backups of existing bouquets"""
        from shutil import copy2
        backup_dir = "/etc/enigma2/backup"
        makedirs(backup_dir, exist_ok=True)

        for f in listdir("/etc/enigma2"):
            if f.startswith(("bouquets.", "userbouquet.")):
                copy2(
                    join("/etc/enigma2", f),
                    join(backup_dir, f)
                )

    def update_main_bouquet(self, groups, keys=None):
        """Update the main bouquet file with generated group bouquets"""
        main_file = "/etc/enigma2/bouquets.tv"
        existing = []
        new_content = []
        marker = "--- | M3UConverter | ---"

        if exists(main_file):
            with open(main_file, 'r+', encoding='utf-8') as f:
                existing = f.readlines()

        # Conserva contenuto esistente prima del marker
        for line in existing:
            if line.strip() == marker.strip():
                break
            new_content.append(line)

        # Aggiungi nuovi gruppi
        new_content.append(marker)
        new_content.append("#NAME " + self.name_file.capitalize() + "\r\n")
        for group in groups:
            group_name = quote(group.replace(' ', '_').replace('/', '_').replace(':', '_')[:50])
            safe_name = self.get_safe_filename(group_name)
            new_content.append(f'#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "userbouquet.{safe_name}.tv" ORDER BY bouquet\n')

        # Scrivi il file
        with open(main_file, 'w') as f:
            f.writelines(new_content)

    def write_group_bouquet(self, group, channels):
        """
        Writes a bouquet file for a single group safely and efficiently.

        This method:
        - Converts the group name into a safe filename by replacing spaces and
          special characters, and truncates it to 50 chars.
        - Writes the bouquet content into a temporary file to avoid corruption.
        - Each channel is written with its service reference (type 4097) and description.
        - Flushes and syncs the file every 50 channels for data safety.
        - Atomically replaces the old bouquet file with the new one.
        - Sets proper file permissions (644).
        - Handles errors by cleaning up temporary files and logging failures.
        """
        try:
            group_name = quote(group.replace(' ', '_').replace('/', '_'), safe='')[:50]
            safe_name = self.get_safe_filename(group_name)
            filename = join("/etc/enigma2", f"userbouquet.{safe_name}.tv")
            temp_file = f"{filename}.tmp"
            name_bouquet = safe_name.capitalize().replace('_', ' ').replace('-', ' ').replace('.', ' ')
            # Scrittura su file temporaneo
            with open(temp_file, 'w', encoding='utf-8', errors='replace') as f:
                f.write(f"#NAME {name_bouquet}\n")
                for idx, ch in enumerate(channels, 1):
                    f.write(f"#SERVICE 4097:0:1:0:0:0:0:0:0:0:{ch['url']}\n")
                    f.write(f"#DESCRIPTION {ch['name']}\n")
                    if idx % 50 == 0:  # Flush periodico
                        f.flush()
                        fsync(f.fileno())

            # Sostituzione atomica del file
            replace(temp_file, filename)
            chmod(filename, 0o644)

        except Exception as e:
            if exists(temp_file):
                try:
                    remove(temp_file)
                except Exception as cleanup_error:
                    logger.error(f"Cleanup error: {cleanup_error}")

            logger.error(f"Failed to write bouquet {group}: {str(e)}")
            raise RuntimeError(f"Bouquet creation failed for {group}") from e

    def get_safe_filename(self, group):
        import unicodedata
        """Generate a secure file name for bouquets"""
        # Normalizza e pulisci il nome del gruppo
        safe_name = unicodedata.normalize('NFKD', group)
        safe_name = safe_name.encode('ascii', 'ignore').decode('ascii')
        safe_name = sub(r'[^a-z0-9_-]', '_', safe_name.lower())
        safe_name = sub(r'_+', '_', safe_name).strip('_')
        # Lunghezza massima 50 caratteri mantenendo l'unicità
        return safe_name[:50] or self.name_file

    def reload_services(self):
        """Reload the list of services"""
        from enigma import eDVBDB
        eDVBDB.getInstance().reloadServicelist()
        eDVBDB.getInstance().reloadBouquets()

    def open_settings(self):
        self.session.open(M3UConverterSettings)

    def show_info(self):
        text = f"M3U Converter Plugin\nVersion {str(currversion)}\nLululla Developed for Enigma2"
        self.session.open(MessageBox, text, MessageBox.TYPE_INFO)

    def exit(self):
        self.close()


class M3UConverterSettings(ConfigListScreen, Screen):
    skin = """
    <screen position="center,center" size="1280,720" title="Settings">
        <widget name="config" position="50,50" size="1180,600" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
        <ePixmap position="20,685" size="200,40" pixmap="skin_default/buttons/red.png" />
        <ePixmap position="280,685" size="200,40" pixmap="skin_default/buttons/green.png" />
        <widget source="key_red" render="Label" position="73,685" size="200,40" zPosition="1" font="Regular;24" />
        <widget source="key_green" render="Label" position="326,685" size="200,40" zPosition="1" font="Regular;24" />
    </screen>"""

    def __init__(self, session):
        Screen.__init__(self, session)
        self["key_red"] = StaticText(_("Cancel"))
        self["key_green"] = StaticText(_("Save"))

        ConfigListScreen.__init__(self, [
            (_("Default folder"), config.plugins.m3uconverter.lastdir),
            (_("Convert HLS streams"), config.plugins.m3uconverter.hls_convert),
            (_("Auto-reload services"), config.plugins.m3uconverter.auto_reload),
            (_("Create backup"), config.plugins.m3uconverter.backup_enable)
        ])

        self["actions"] = ActionMap(["OkCancelActions", "ColorActions"], {
            "green": self.save,
            "red": self.cancel,
            "cancel": self.cancel
        }, -1)

    def save(self):
        for x in self["config"].list:
            x[1].save()
        self.close(True)

    def cancel(self):
        self.close(False)


def main(session, **kwargs):
    session.open(M3UConverter)


def Plugins(**kwargs):
    from Plugins.Plugin import PluginDescriptor
    return [PluginDescriptor(
        name=_("M3U Converter"),
        description=_("Archimede Convert M3U playlists to Enigma2 bouquets"),
        where=PluginDescriptor.WHERE_PLUGINMENU,
        icon="plugin.png",
        fnc=main)
    ]
