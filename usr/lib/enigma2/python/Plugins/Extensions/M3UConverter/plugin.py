# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 1.9                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "17:30 - 20250903"                    #
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

# Standard library
import codecs
import json
import shutil
import unicodedata
from gettext import gettext as _
from os import access, W_OK, listdir, remove, replace, chmod, fsync, system, mkdir
from os.path import exists, isdir, isfile, join, normpath, basename, dirname
from re import sub, findall, DOTALL, MULTILINE, IGNORECASE, search
from threading import Lock
from time import strftime
from urllib.parse import unquote

# Third-party libraries
from twisted.internet import threads
from collections import defaultdict

# Enigma2 core
from enigma import eDVBDB, eServiceReference, getDesktop, eTimer

# Enigma2 components
from Components.ActionMap import ActionMap
from Components.FileList import FileList
from Components.Label import Label
from Components.MenuList import MenuList
from Components.Sources.Progress import Progress
from Components.Sources.StaticText import StaticText
from Components.config import config, ConfigSelection, ConfigSubsection, ConfigYesNo, ConfigNumber
from Screens.MessageBox import MessageBox
from Screens.Screen import Screen
from Screens.Setup import Setup
from Tools.Directories import defaultRecordingLocation, fileExists
from enigma import eAVControl as AVSwitch

from .Logger_clr import ColoredLogger


class AspectManager:

    def __init__(self):
        self.init_aspect = self.get_current_aspect()
        print("[INFO] Initial aspect ratio:", self.init_aspect)

    def get_current_aspect(self):
        """Returns the current aspect ratio of the device."""
        try:
            return int(AVSwitch().getAspectRatioSetting())
        except Exception as e:
            print("[ERROR] Failed to get aspect ratio:", str(e))
            return 0

    def restore_aspect(self):
        """Restores the original aspect ratio when the plugin exits."""
        try:
            print("[INFO] Restoring aspect ratio to:", self.init_aspect)
            AVSwitch().setAspectRatio(self.init_aspect)
        except Exception as e:
            print("[ERROR] Failed to restore aspect ratio:", str(e))


# for my friend Archimede
currversion = '1.9'
last_date = "20250903"
title_plug = _("Archimede Universal Converter v.%s by Lululla") % currversion
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2


aspect_manager = AspectManager()
screenwidth = getDesktop(0).size()
screen_width = screenwidth.width()

archimede_converter_path = "archimede_converter"
log_dir = join("/tmp", archimede_converter_path)
main_log = join(log_dir, "unified_converter.log")

# Create directory if it does not exist
try:
    from os import makedirs
    makedirs(log_dir, exist_ok=True)
except Exception:
    pass

# Global instance with dual output
logger = ColoredLogger(
    log_file=join(log_dir, "m3u_converter.log"),
    secondary_log=join(log_dir, "debug.log"),
    clear_on_start=False
)


# Ensure movie path
def defaultMoviePath():
    result = config.usage.default_path.value
    if not result.endswith("/"):
        result += "/"
    if not isdir(result):
        return defaultRecordingLocation(config.usage.default_path.value)
    return result


def get_mounted_devices():

    basic_paths = [
        ("/media/hdd/", _("HDD Drive")),
        ("/media/usb/", _("USB Drive")),
        ("/media/ba/", _("Barry Allen")),
        ("/media/net/", _("Network Storage")),
        ("/tmp/", _("Temporary"))
    ]

    # Check which paths exist and are writable
    valid_devices = []
    for path, desc in basic_paths:
        if isdir(path) and access(path, W_OK):
            valid_devices.append((path, desc))

    # Add additional USB devices if available (usb1, usb2...)
    for i in range(1, 4):
        usb_path = "/media/usb%d/" % i
        if isdir(usb_path) and access(usb_path, W_OK):
            valid_devices.append((usb_path, _("USB Drive") + " %d" % i))

    return valid_devices


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

config.plugins.m3uconverter.epg_enabled = ConfigYesNo(default=True)
config.plugins.m3uconverter.epg_source = ConfigSelection(
    default="rytec",
    choices=[("rytec", "Rytec"), ("internal", "Internal Mapping")]
)
config.plugins.m3uconverter.bouquet_mode = ConfigSelection(
    default="multi",
    choices=[("single", _("Single Bouquet")), ("multi", _("Multiple Bouquets"))]
)
config.plugins.m3uconverter.bouquet_position = ConfigSelection(
    default="bottom",
    choices=[("top", _("Top")), ("bottom", _("Bottom"))]
)
config.plugins.m3uconverter.hls_convert = ConfigYesNo(default=True)
config.plugins.m3uconverter.filter_dead_channels = ConfigYesNo(default=False)
config.plugins.m3uconverter.auto_reload = ConfigYesNo(default=True)
config.plugins.m3uconverter.backup_enable = ConfigYesNo(default=True)
config.plugins.m3uconverter.max_backups = ConfigNumber(default=3)

update_mounts()


def create_backup():
    """Create a backup of bouquets.tv only"""
    from shutil import copy2
    src = "/etc/enigma2/bouquets.tv"
    dst = "/etc/enigma2/bouquets.tv.bak"
    if exists(src):
        copy2(src, dst)


def reload_services_orig():
    """Reload the list of services"""
    eDVBDB.getInstance().reloadServicelist()
    eDVBDB.getInstance().reloadBouquets()


def reload_services():
    """Reload bouquets in Enigma2 with multiple fallback methods"""
    try:
        import time
        import subprocess

        # Method 1: Use eDVBDB to reload bouquets (most reliable)
        try:
            from enigma import eDVBDB
            db = eDVBDB.getInstance()
            if db:
                db.reloadBouquets()
                logger.info("Bouquets reloaded using eDVBDB")
                time.sleep(2)  # Wait for reload to complete
                return True
        except Exception as e:
            logger.warning("eDVBDB reload failed: %s", str(e))

        # Method 2: Use web interface reload
        try:
            result = subprocess.run([
                'wget',
                '--timeout=10',
                '--tries=2',
                '-qO-',
                'http://127.0.0.1/web/servicelistreload?mode=0'
            ], capture_output=True, text=True, timeout=15)

            if result.returncode == 0:
                logger.info("Bouquets reloaded via web interface")
                time.sleep(2)
                return True
        except Exception as e:
            logger.warning("Web interface reload failed: %s", str(e))

        # Method 3: Use enigma2 restart script
        try:
            result = subprocess.run([
                'systemctl', 'restart', 'enigma2'
            ], capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                logger.info("Enigma2 restarted via systemctl")
                time.sleep(5)
                return True
        except:
            pass

        # Method 4: Send HUP signal
        try:
            result = subprocess.run([
                'pkill', '-HUP', 'enigma2'
            ], capture_output=True, text=True, timeout=10)

            logger.info("Sent HUP signal to enigma2")
            time.sleep(3)
            return True
        except Exception as e:
            logger.warning("HUP signal failed: %s", str(e))

        logger.error("All bouquet reload methods failed")
        return False

    except Exception as e:
        logger.error("Error reloading bouquets: %s", str(e))
        return False


def transliterate(text):
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode('ascii', 'ignore').decode('ascii')


def clean_group_name(name):
    return name.encode("ascii", "ignore").decode().replace(" ", "_").replace("/", "_").replace(":", "_")[:50]


class EPGServiceMapper:
    def __init__(self, prefer_satellite=True):
        self.prefer_satellite = prefer_satellite
        self.channel_map = defaultdict(list)
        self.rytec_map = {}
        self.rytec_clean_map = {}
        self._clean_name_cache = {}
        self.enigma_config = self.load_enigma2_config()
        self.country_code = self.get_country_code()
        self.optimized_channel_map = {}

        logger.info("EPGServiceMapper initialized with prefer_satellite=%s, country_code=%s",
                    prefer_satellite, self.country_code)

    def load_enigma2_config(self, settings_path="/etc/enigma2/settings"):
        """Load Enigma2 configuration to determine configured satellites"""
        config_data = {'satellites': set(), 'terrestrial': False, 'cable': False}

        if not fileExists(settings_path):
            logger.warning("Enigma2 settings file not found: %s", settings_path)
            return config_data

        try:
            with open(settings_path, 'r') as f:
                lines = f.readlines()

            for line in lines:
                line = line.strip()
                if line.startswith('config.Nims.') and '.dvbs.diseqc' in line:
                    parts = line.split('=')
                    if len(parts) == 2 and parts[1].isdigit():
                        sat_position = int(parts[1])
                        if sat_position > 0:
                            config_data['satellites'].add(sat_position)
                elif line.startswith('config.Nims.') and '.dvbt.terrestrial' in line:
                    config_data['terrestrial'] = True
                elif line.startswith('config.Nims.') and '.dvbc.configMode' in line:
                    config_data['cable'] = True

            logger.info("Enigma2 configuration loaded: %s", config_data)
            return config_data
        except Exception as e:
            logger.error("Error reading Enigma2 settings: %s", str(e))
            return config_data

    def is_service_compatible(self, service_ref):
        """Check if service is compatible with current configuration"""
        if not service_ref or service_ref.startswith('4097:'):
            return True

        parts = service_ref.split(':')
        if len(parts) < 11:
            return False

        try:
            onid = int(parts[5], 16) if parts[5] else 0
            service_type = parts[2]
            onid_to_position = {
                0x13E: 130,     # 13.0°E Hotbird
                0x110: 130,     # 13.0°E Eutelsat
                0x1:   192,     # 19.2°E Astra
                0x2:   282,     # 28.2°E Astra
                0x11:  235,     # 23.5°E Astra
                0x10:   90,     # 9.0°E Eutelsat 9B
                0x212: 315,     # 31.5°E Astra
                0x204: 330,     # 33.0°E Eutelsat
                0x3:    360,    # 36.0°E Eutelsat
                0x42:   420,    # 42.0°E Turksat
                0x100:  450,    # 45.0°E Azerspace
                0x318:  160,    # 16.0°E Eutelsat 16A
                0x20:   260,    # 26.0°E Badr
                0x30:   255,    # 25.5°E Es'hailSat
                0x200:   70,    # 7.0°E Eutelsat 7
                0x400:  1500,   # 15.0°W Intelsat
                0x500:   50,    # 5.0°W Eutelsat 5W
                0x600:  3000,   # 30.0°W Hispasat
                0x700:   19,    # 1.9°E Astra
                0x800:    8,    # 0.8°W Thor / Intelsat
                0x900:  800,    # 8.0°W Eutelsat 8W
                0xA00:   30,    # 3.0°E Eutelsat 3B
                0xB00:   48,    # 4.8°E Astra 4A
                0xC00:   70,    # 7.0°W Nilesat
                0xFFFF:  0,     # Servizi via cavo (ONID speciale)
                0xEEEE:  0,     # Servizi terrestri (ONID speciale)
                # # --- DVB-C (Cavo) ---
                # 0xFFFF: 0     # Cavo generico
                # 0x2184: 0,    # DVB-C Germania
                # 0x22F1: 0,    # DVB-C Olanda
                # 0x233A: 0,    # DVB-C Svizzera
                # 0x20D0: 0,    # DVB-C Austria
                # 0x22C1: 0,    # DVB-C Belgio

                # # --- DVB-T/T2 (Terrestre) ---
                # 0x2170: 0,    # DVB-T Italia (Mediaset)
                # 0x2171: 0,    # DVB-T Italia (RAI)
                # 0x20FA: 0,    # DVB-T Germania
                # 0x22F0: 0,    # DVB-T Olanda
                # 0x22C0: 0,    # DVB-T Belgio
                # 0x233B: 0,    # DVB-T Svizzera
                # 0x22F2: 0,    # DVB-T Regno Unito
            }

            if onid in onid_to_position:
                sat_position = onid_to_position[onid]
                if sat_position > 0:
                    return sat_position in self.enigma_config['satellites']
                elif sat_position == 0:
                    if onid == 0xFFFF and self.enigma_config['cable']:
                        return True
                    elif onid == 0xEEEE and self.enigma_config['terrestrial']:
                        return True

            if service_type == '16' and self.enigma_config['terrestrial']:
                return True
            elif service_type == '10' and self.enigma_config['cable']:
                return True
        except (ValueError, IndexError):
            pass

        return False

    def classify_service(self, sref):
        """Classify service type based on service reference"""
        if not sref:
            return "unknown"

        if sref.startswith("1:0:1:") or sref.startswith("1:0:2:") or sref.startswith("1:0:19:"):
            return "satellite"

        elif sref.startswith("1:0:16:"):
            return "dvb-t"

        elif sref.startswith("1:0:10:"):
            return "dvb-c"

        # IPTV (Streaming)
        elif sref.startswith("4097:"):
            return "iptv"

        # Marker or separator
        elif "0:0:0:0:0:0:0:0:0" in sref:
            return "marker"

        # Special services (PVR, recording, etc.)
        elif sref.startswith("1:0:0:") or sref.startswith("1:134:"):
            return "special"

        return "unknown"

    def get_country_code(self):
        """Get country code from Enigma2 settings"""
        country_code = "it"  # Default to Italy
        settings_path = "/etc/enigma2/settings"
        if not fileExists(settings_path):
            logger.warning("Enigma2 settings file not found, using default country code: %s", country_code)
            return country_code

        try:
            with open(settings_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('config.misc.country='):
                        country_code = line.split('=', 1)[1].strip()
                        break
            return country_code.lower()
        except Exception as e:
            logger.error("Error reading Enigma2 settings: {0}".format(str(e)))
            return country_code

    def clean_channel_name(self, name):
        """Clean channel name for matching"""
        if not name:
            return ""

        if name in self._clean_name_cache:
            return self._clean_name_cache[name]

        try:
            cleaned = name.lower().strip()
            cleaned = sub(r'^\|[a-z]{2}\|\s*', '', cleaned)
            cleaned = sub(r'^[a-z]{2}:\s*', '', cleaned)

            quality_patterns = [
                r'\b(4k|uhd|fhd|hd|sd|hq|uhq|sdq|hevc|h265|h264|h\.265|h\.264)\b',
                r'\b(full hd|ultra hd|high definition|standard definition)\b',
                r'\s*\(\d+p\)', r'\s*\d+p', r'\s*\[.*?\]', r'\s*\(.*?\)'
            ]

            for pattern in quality_patterns:
                cleaned = sub(pattern, '', cleaned, flags=IGNORECASE)

            for char in '()[]{}|/\\_—–-':
                cleaned = cleaned.replace(char, ' ')

            prefixes = ['canale', 'channel', 'tv', 'tele', 'it-', 'it_', 'the ']
            for prefix in prefixes:
                if cleaned.startswith(prefix) and not cleaned[len(prefix):].strip()[0].isdigit():
                    cleaned = cleaned[len(prefix):].strip()

            cleaned = sub(r'[^a-z0-9\s]', '', cleaned)
            cleaned = ' '.join(cleaned.split()).strip()

            if not cleaned:
                cleaned = sub(r'[^a-z0-9]', '', name.lower())
                logger.warning("Channel name '{0}' resulted in empty string, using fallback: '{1}'".format(name, cleaned))

            self._clean_name_cache[name] = cleaned
            logger.debug("Cleaned channel name: '{0}' -> '{1}'".format(name, cleaned))
            return cleaned
        except Exception as e:
            logger.error("Error cleaning channel name '{0}': {1}".format(name, str(e)))
            return sub(r'[^a-z0-9]', '', name.lower())

    def optimize_matching(self):
        """Optimize channel map structures for faster matching with satellite preference"""
        self.optimized_channel_map = {}

        for name, services in self.channel_map.items():
            if not services:
                continue

            # Filter out terrestrial services if we prefer satellite
            if self.prefer_satellite:
                services = [s for s in services if s["type"] != "dvb-t"]

            if not services:
                continue

            # Prefer satellite services over cable
            satellite_services = [s for s in services if s["type"] == "satellite"]
            cable_services = [s for s in services if s["type"] == "dvb-c"]

            # Select the best service (satellite first, then cable)
            if satellite_services:
                main_service = satellite_services[0]
            elif cable_services:
                main_service = cable_services[0]
            else:
                main_service = services[0]

            clean_name = self.clean_channel_name(name)

            # Store both original and cleaned name for faster lookup
            self.optimized_channel_map[name] = main_service
            if clean_name not in self.optimized_channel_map:
                self.optimized_channel_map[clean_name] = main_service

        logger.info("Optimized channel map built: {0} entries".format(len(self.optimized_channel_map)))

    def parse_lamedb(self, lamedb_path="/etc/enigma2/lamedb"):
        """Parse both lamedb and lamedb5 for full coverage with 16-bit ONID"""
        paths_to_try = [
            "/etc/enigma2/lamedb5",
            "/etc/enigma2/lamedb"
        ]

        for lamedb_path in paths_to_try:
            if not fileExists(lamedb_path):
                continue

            try:
                with open(lamedb_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                # Identify the file format
                if content.startswith("eDVB services /5/"):
                    # lamedb5 format
                    lines = content.split("\n")
                    for line in lines:
                        if line.startswith("s:"):
                            parts = line.split(",")
                            if len(parts) >= 2:
                                sref_parts = parts[0].split(":")
                                if len(sref_parts) >= 7:
                                    service_id = sref_parts[1]
                                    ts_id = sref_parts[2]
                                    on_id = sref_parts[3]
                                    namespace = sref_parts[6]

                                    # Convert to 16-bit ONID
                                    if len(on_id) > 4:
                                        on_id = on_id[:4]  # Truncate to 4 hex digits

                                    service_type = sref_parts[4]

                                    # Create service reference with 16-bit ONID
                                    sref = "1:0:{0}:{1}:{2}:{3}:{4}:0:0:0:".format(
                                        service_type, service_id, ts_id, on_id, namespace
                                    )
                                    channel_name = parts[1].strip('"')
                                    clean_name = self.clean_channel_name(channel_name)

                                    self.channel_map[clean_name].append({
                                        "sref": sref,
                                        "type": self.classify_service(sref),
                                        "source": "lamedb5",
                                        "service_id": service_id,
                                        "ts_id": ts_id,
                                        "on_id": on_id
                                    })
                else:
                    # Traditional lamedb format
                    lines = content.split("\n")
                    for line in lines:
                        if line.startswith("s:"):
                            parts = line.split(",")
                            if len(parts) >= 2:
                                # Extract service reference and channel name
                                sref_part = parts[0]
                                channel_name = parts[1].strip('"')

                                # Extract components from service reference
                                sref_parts = sref_part.split(":")
                                if len(sref_parts) >= 6:
                                    service_id = sref_parts[1]
                                    on_id = sref_parts[2]
                                    # Convert to 16-bit ONID
                                    if len(on_id) > 4:
                                        on_id = on_id[:4]  # Truncate to 4 hex digits

                                    ts_id = sref_parts[3]
                                    service_type = sref_parts[4]

                                    sref = "1:0:{0}:{1}:{2}:{3}:820000:0:0:0:".format(
                                        service_type, service_id, ts_id, on_id
                                    )
                                    clean_name = self.clean_channel_name(channel_name)

                                    self.channel_map[clean_name].append({
                                        "sref": sref,
                                        "type": self.classify_service(sref),
                                        "source": "lamedb",
                                        "service_id": service_id,
                                        "ts_id": ts_id,
                                        "on_id": on_id
                                    })

                # After populating self.channel_map, filter incompatible services
                for name in list(self.channel_map.keys()):
                    compatible_services = self.filter_compatible_services(self.channel_map[name])
                    if compatible_services:
                        self.channel_map[name] = compatible_services
                    else:
                        del self.channel_map[name]

                logger.info("Parsed {0} unique compatible DVB channel names from {1}".format(len(self.channel_map), lamedb_path))
                return True

            except Exception as e:
                logger.error("Error parsing {0}: {1}".format(lamedb_path, str(e)))

        logger.error("Could not find or parse any lamedb file")
        return False

    def parse_rytec_channels(self, rytec_path="/etc/epgimport/rytec.channels.xml"):
        """Parse rytec.channels.xml with service type correction"""
        self.rytec_map = {}
        self.rytec_clean_map = {}

        if not fileExists(rytec_path):
            logger.warning("rytec.channels.xml file not found: %s", rytec_path)
            return

        try:
            with open(rytec_path, "r", encoding="utf-8") as f:
                content = f.read()

            channel_pattern = r'<channel id="([^"]+)">([^<]+)</channel>'
            matches = findall(channel_pattern, content, IGNORECASE)

            for channel_id, service_ref in matches:
                # Normalize the service reference with correct namespace
                normalized_ref = self.normalize_service_reference(service_ref)

                # Only keep compatible services
                if self.is_service_compatible(normalized_ref):
                    self.rytec_map[channel_id] = normalized_ref

                    # Extract base channel name for matching
                    base_id = channel_id.split(".")[0] if "." in channel_id else channel_id
                    clean_base_id = self.clean_channel_name(base_id)
                    self.rytec_clean_map[clean_base_id] = normalized_ref

            logger.info(
                "Parsed %d compatible DVB service references from rytec.channels.xml",
                len(self.rytec_map)
            )

        except Exception as e:
            logger.error("Error parsing rytec.channels.xml: %s", str(e))

    def filter_compatible_services(self, services):
        """Keep only compatible service references"""
        compatible_services = []
        for service in services:
            if self.is_service_compatible(service['sref']):
                compatible_services.append(service)
        return compatible_services

    def find_best_service_match(self, clean_name, tvg_id=None):
        # Exact match first
        if tvg_id and tvg_id in self.rytec_map:
            return self.rytec_map[tvg_id], 'rytec_exact'

        # Try name variations
        name_variations = self.generate_name_variations(clean_name)
        for variation in name_variations:
            if variation in self.rytec_clean_map:
                return self.rytec_clean_map[variation], 'rytec_variation'
            if variation in self.optimized_channel_map:
                return self.optimized_channel_map[variation]['sref'], 'lamedb_variation'

        return None, None

    def find_best_service_match2(self, clean_name, tvg_id=None):
        """Find best service match considering compatibility"""
        matches = []

        logger.debug("Searching for: '%s' (tvg_id: %s)", clean_name, tvg_id)

        # Exact match in rytec by tvg_id
        if tvg_id and tvg_id in self.rytec_map:
            sref = self.rytec_map[tvg_id]
            if self.is_service_compatible(sref):
                logger.debug("Found exact tvg_id match: %s -> %s", tvg_id, sref)
                matches.append((sref, 'rytec_exact', 100))

        # Clean name match in rytec
        if clean_name in self.rytec_clean_map:
            sref = self.rytec_clean_map[clean_name]
            if self.is_service_compatible(sref):
                logger.debug("Found clean name match in rytec: %s -> %s", clean_name, sref)
                matches.append((sref, 'rytec_clean', 90))

        # Name variations in rytec
        name_variations = self.generate_name_variations(clean_name)
        for variation in name_variations:
            if variation in self.rytec_clean_map:
                sref = self.rytec_clean_map[variation]
                if self.is_service_compatible(sref):
                    logger.debug("Found variation match in rytec: %s -> %s", variation, sref)
                    matches.append((sref, 'rytec_variation', 85))

        # Partial matches in rytec
        for rytec_name, rytec_sref in self.rytec_clean_map.items():
            if clean_name in rytec_name or rytec_name in clean_name:
                similarity = self.calculate_similarity(clean_name, rytec_name)
                if similarity > 60 and self.is_service_compatible(rytec_sref):
                    logger.debug("Found partial match in rytec: %s ~ %s -> %s", clean_name, rytec_name, rytec_sref)
                    matches.append((rytec_sref, 'rytec_partial', similarity))

        # Exact match in lamedb
        if clean_name in self.optimized_channel_map:
            service = self.optimized_channel_map[clean_name]
            if self.is_service_compatible(service['sref']):
                logger.debug("Found exact match in lamedb: %s -> %s", clean_name, service['sref'])
                matches.append((service['sref'], 'lamedb_exact', 95))

        # Partial matches in lamedb
        for lamedb_name, service in self.optimized_channel_map.items():
            if clean_name in lamedb_name or lamedb_name in clean_name:
                similarity = self.calculate_similarity(clean_name, lamedb_name)
                if similarity > 60 and self.is_service_compatible(service['sref']):
                    logger.debug("Found partial match in lamedb: %s ~ %s -> %s", clean_name, lamedb_name, service['sref'])
                    matches.append((service['sref'], 'lamedb_partial', similarity))

        # Return best match by score
        if matches:
            matches.sort(key=lambda x: x[2], reverse=True)
            logger.debug("Best match for '%s': %s with score %d", clean_name, matches[0][1], matches[0][2])
            return matches[0][0], matches[0][1]

        logger.debug("No compatible matches found for: '%s'", clean_name)
        return None, None

    def generate_name_variations(self, name):
        """Generate variations of a channel name for matching"""
        variations = set()
        variations.add(name)
        variations.add(name.replace(' ', ''))
        variations.add(name.replace(' ', '_'))
        variations.add(sub(r'\d+', '', name))
        return variations

    def calculate_similarity(self, name1, name2):
        """Compute similarity between two names"""
        if not name1 or not name2:
            return 0

        if name1 in name2 or name2 in name1:
            return 0.8

        try:
            from difflib import SequenceMatcher
            return SequenceMatcher(None, name1, name2).ratio()
        except ImportError:
            common_chars = set(name1) & set(name2)
            return len(common_chars) / max(len(set(name1)), len(set(name2)))

    def normalize_service_reference(self, sref, for_epg=False):
        """Normalize service reference with correct namespace"""
        if not sref or not isinstance(sref, str):
            return sref

        if sref.startswith('4097:'):
            parts = sref.split(':')
            if len(parts) < 11:
                parts += ['0'] * (11 - len(parts))

            if len(parts) >= 11 and parts[2] != '0' and parts[3] != '0' and parts[4] != '0' and parts[5] != '0':
                parts[6] = '820000'

            return ':'.join(parts)

        parts = sref.split(':')
        if len(parts) < 11:
            parts += ['0'] * (11 - len(parts))

        if len(parts) > 6:
            parts[6] = '820000'

        if for_epg and len(parts) > 5:
            onid = parts[5]
            if len(onid) > 4:
                parts[5] = onid[:4]

        return ':'.join(parts)

    def generate_hybrid_sref(self, dvb_sref, url=None, for_epg=False):
        """Generate hybrid service reference for bouquet or EPG"""
        if not dvb_sref:
            if url:
                return self.generate_service_reference(url)
            return None

        # If the service reference is already IPTV (4097), handle directly
        if dvb_sref.startswith('4097:'):
            if for_epg:
                # For EPG, remove URL if present
                parts = dvb_sref.split(':')
                if len(parts) > 10:
                    return ':'.join(parts[:10]) + ':'
            return dvb_sref

        # Normalize DVB reference
        dvb_sref = self.normalize_service_reference(dvb_sref, for_epg)

        if for_epg:
            # For EPG, do not include URL
            return dvb_sref

        # Convert DVB reference to IPTV format for bouquet
        if dvb_sref.startswith('1:0:'):
            parts = dvb_sref.split(':')
            if len(parts) >= 11:
                service_type = parts[2]
                service_id = parts[3]
                ts_id = parts[4]
                on_id = parts[5]
                namespace = parts[6]

                # Ensure namespace is correct
                if namespace in ['0', '00000000']:
                    namespace = '820000'

                base_sref = f"4097:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

                if url:
                    encoded_url = url.replace(':', '%3a').replace(' ', '%20')
                    return base_sref + encoded_url
                return base_sref

        # If not a valid DVB reference, generate pure IPTV reference
        return self.generate_service_reference(url) if url else None

    def generate_service_reference(self, url):
        """Generate IPTV service reference (4097) with URL encoding"""
        encoded_url = url.replace(':', '%3a')
        encoded_url = encoded_url.replace(' ', '%20')
        encoded_url = encoded_url.replace('?', '%3f')
        encoded_url = encoded_url.replace('=', '%3d')
        encoded_url = encoded_url.replace('&', '%26')
        encoded_url = encoded_url.replace('#', '%23')
        sref = "4097:0:1:0:0:0:0:0:0:0:%s" % encoded_url
        logger.debug("Generated service reference: %s", sref)
        return sref

    def generate_epg_channels_file(self, epg_data, bouquet_name):
        """Generate EPG file for epgimport in the proper directory"""
        epgimport_path = "/etc/epgimport"
        epg_filename = "%s.channels.xml" % bouquet_name
        epg_path = join(epgimport_path, epg_filename)

        if not fileExists(epgimport_path):
            try:
                mkdir(epgimport_path)
                logger.info("Created epgimport directory: %s", epgimport_path)
            except Exception as e:
                logger.error("Could not create epgimport directory: %s", str(e))
                return False

        try:
            with open(epg_path, 'w', encoding="utf-8") as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<channels>\n')

                for channel in epg_data:
                    if channel.get('tvg_id') and channel.get('sref'):
                        epg_sref = self.normalize_service_reference(channel['sref'], True)
                        # Ensure the service reference is in the correct format for EPG
                        if epg_sref.startswith('4097:'):
                            # For IPTV service reference, remove the URL part if present
                            parts = epg_sref.split(':')
                            if len(parts) > 10:
                                epg_sref = ':'.join(parts[:10]) + ':'
                        f.write('  <channel id="%s">%s</channel>\n' % (channel["tvg_id"], epg_sref))
                    else:
                        logger.warning("Skipping channel due to missing tvg_id or sref: %s", channel.get('name'))
                f.write('</channels>\n')

            logger.info("Generated EPG channels file: %s", epg_path)
            return True
        except Exception as e:
            logger.error("Error generating EPG channels file: %s", str(e))
            return False

    def generate_epg_sources_file(self, bouquet_name, epg_url=None):
        """Generate EPG sources file for epgimport in the proper format"""
        epgimport_path = "/etc/epgimport"
        sources_filename = "ArchimedeConverter.sources.xml"
        sources_path = join(epgimport_path, sources_filename)

        # Create directory if missing
        if not fileExists(epgimport_path):
            try:
                mkdir(epgimport_path)
                logger.info("Created epgimport directory: %s", epgimport_path)
            except Exception as e:
                logger.error("Could not create epgimport directory: %s", str(e))
                return False

        try:
            # Read existing file or initialize new
            if fileExists(sources_path):
                with open(sources_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Remove old source for this bouquet
                pattern = r'<source type="gen_xmltv"[^>]*channels="%s\.channels\.xml"[^>]*>.*?</source>' % bouquet_name
                content = sub(pattern, '', content, flags=DOTALL)

                # Remove empty sourcecats
                content = sub(r'<sourcecat[^>]*>\s*</sourcecat>', '', content)
            else:
                content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n</sources>'

            # Add new source
            sourcecat_marker = r'<sourcecat sourcecatname="Archimede Converter by Lululla">'
            if sourcecat_marker in content:
                # Append to existing sourcecat
                new_source = '    <source type="gen_xmltv" nocheck="1" channels="%s.channels.xml">\n' % bouquet_name
                new_source += '      <description>%s</description>\n' % bouquet_name
                if epg_url:
                    new_source += '      <url><![CDATA[%s]]></url>\n' % epg_url
                else:
                    new_source += '      <url>http://www.xmltvepg.nl/rytecIPTV.xz</url>\n'
                    new_source += '      <url>https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz</url>\n'
                    new_source += '      <url>http://rytecepg.wanwizard.eu/rytecIPTV.xz</url>\n'
                new_source += '    </source>\n'
                content = content.replace(sourcecat_marker, sourcecat_marker + '\n' + new_source)
            else:
                # Create new sourcecat
                new_sourcecat = '  <sourcecat sourcecatname="Archimede Converter by Lululla">\n'
                new_sourcecat += '    <source type="gen_xmltv" nocheck="1" channels="%s.channels.xml">\n' % bouquet_name
                new_sourcecat += '      <description>%s</description>\n' % bouquet_name
                if epg_url:
                    new_sourcecat += '      <url><![CDATA[%s]]></url>\n' % epg_url
                else:
                    new_sourcecat += '      <url>http://www.xmltvepg.nl/rytecIPTV.xz</url>\n'
                    new_sourcecat += '      <url>https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz</url>\n'
                    new_sourcecat += '      <url>http://rytecepg.wanwizard.eu/rytecIPTV.xz</url>\n'
                new_sourcecat += '    </source>\n'
                new_sourcecat += '  </sourcecat>\n'
                content = content.replace('</sources>', new_sourcecat + '</sources>')

            # Write back
            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(content)

            logger.info("Generated/Updated EPG sources file: %s", sources_path)
            return True
        except Exception as e:
            logger.error("Error generating EPG sources file: %s", str(e))
            return False

    def extract_epg_url_from_m3u(self, m3u_path):
        """Search for an EPG URL in M3U file comments"""
        try:
            with open(m3u_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # Look for EPG URL in comments
            epg_match = search(r'#EXTEPGURL:?(.*)$', content, MULTILINE)
            if epg_match:
                return epg_match.group(1).strip()

            # Also check other common formats
            epg_match = search(r'#EXTVLCOPT:epg-url=(.*)$', content, MULTILINE)
            if epg_match:
                return epg_match.group(1).strip()

            return None
        except:
            return None

    def initialize(self):
        """Initialize the mapper by loading all necessary data"""
        try:
            self.parse_lamedb()
            self.parse_rytec_channels()
            self.optimize_matching()
            logger.info("EPGServiceMapper initialized successfully")
            return True
        except Exception as e:
            logger.error("Failed to initialize EPGServiceMapper: %s", str(e))
            return False


class CoreConverter:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        if not self.__initialized:
            self.backup_dir = join(archimede_converter_path, "archimede_backup")
            self.log_file = join(archimede_converter_path, "archimede_converter.log")

            self.__create_dirs()
            self.__initialized = True

    def __create_dirs(self):
        """Create necessary directories if they don't exist"""
        try:
            makedirs(self.backup_dir, exist_ok=True)
        except Exception as e:
            print(f"Error creating directories: {str(e)}")

    def safe_convert(self, func, *args, **kwargs):
        """Performs a conversion with automatic backup"""
        try:
            self._create_backup()
            result = func(*args, **kwargs)
            self._log_success(func.__name__)
            return result
        except Exception as e:
            self._log_error(e)
            self._restore_backup()
            raise RuntimeError(f"Conversion failed (restored backup). Error: {str(e)}")

    def _create_backup(self):
        """Create a backup of the existing bouquets"""
        try:
            if not exists("/etc/enigma2/bouquets.tv"):
                return

            timestamp = strftime("%Y%m%d_%H%M%S")
            backup_file = join(self.backup_dir, f"bouquets_{timestamp}.tv")
            shutil.copy2("/etc/enigma2/bouquets.tv", backup_file)
        except Exception as e:
            raise RuntimeError(f"Backup failed: {str(e)}")

    def _restore_backup(self):
        """Restore the most recent available backup"""
        try:
            backups = sorted([f for f in listdir(self.backup_dir)
                              if f.startswith("bouquets_") and f.endswith(".tv")])

            if backups:
                latest = join(self.backup_dir, backups[-1])
                shutil.copy2(latest, "/etc/enigma2/bouquets.tv")
        except Exception as e:
            raise RuntimeError(f"Restore failed: {str(e)}")

    def _log_success(self, operation):
        """Log a successful operation"""
        msg = f"{strftime('%Y-%m-%d %H:%M:%S')} [SUCCESS] {operation}"
        self.__write_log(msg)

    def _log_error(self, error):
        """Log an error"""
        msg = f"{strftime('%Y-%m-%d %H:%M:%S')} [ERROR] {str(error)}"
        self.__write_log(msg)

    def __write_log(self, message):
        """Write to the log file"""
        try:
            with open(self.log_file, "a") as f:
                f.write(message + "\n")
        except Exception:
            print(f"Fallback log: {message}")

    def filter_channels(self, channels, filter_type="all"):
        """Filter channels by type"""
        if not channels:
            return []

        if filter_type == "working":
            return [ch for ch in channels if self._is_url_alive(ch.get("url", ""))]
        return channels

    def _is_url_alive(self, url, timeout=5):
        """Check if a URL is reachable"""
        if not url:
            return False

        try:
            cmd = f"curl --max-time {timeout} --head --silent --fail --output /dev/null {url}"
            return system(cmd) == 0
        except Exception:
            return False

    def cleanup_old_backups(self, max_backups=5):
        """Keep only the latest N backups"""
        try:
            backups = sorted([f for f in listdir(self.backup_dir)
                              if f.startswith("bouquets_") and f.endswith(".tv")])

            for old_backup in backups[:-max_backups]:
                remove(join(self.backup_dir, old_backup))
        except Exception as e:
            self._log_error(f"Cleanup failed: {str(e)}")


core_converter = CoreConverter()


class M3UFileBrowser(Screen):
    def __init__(self, session, startdir="/etc/enigma2", matchingPattern=r"(?i)^.*\.tv$", conversion_type=None, title=None):
        Screen.__init__(self, session)
        if title:
            self.setTitle(title)
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
            self.onShown.append(self._filter_list)

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

    def close(self, result=None):
        try:
            # self.session.nav.stopService()
            super(M3UFileBrowser, self).close(result)
        except Exception as e:
            logger.error(f"Error closing browser: {str(e)}")
            super(M3UFileBrowser, self).close(None)


class ConversionSelector(Screen):

    skin = """
        <screen name="ConversionSelector" position="center,center" size="1280,720" title="..::ConversionSelector::.." backgroundColor="#20000000" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <widget name="list" position="15,135" size="1250,491" scrollbarMode="showOnDemand" transparent="1" itemHeight="35" zPosition="5" foregroundColor="#00a0a0a0" foregroundColorSelected="#ffffff" backgroundColor="#20000000" backgroundColorSelected="#0b2049" />
            <widget name="status" position="14,79" size="1250,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" zPosition="5" />

            <widget name="info" position="0,0" size="1,1" font="Regular;1" transparent="1" />
            <widget name="text" position="0,0" size="1,1" font="Regular;1" transparent="1" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="31,685" size="280,6" zPosition="12" />
            <widget name="key_red" position="30,645" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="30,645" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="338,685" size="280,6" zPosition="12" />
            <widget name="key_green" position="340,645" size="280,45" zPosition="11" font="Regular; 30" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="340,645" size="280,45" zPosition="11" font="Regular; 30" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="645,685" size="280,6" zPosition="12" />
            <widget name="key_yellow" position="645,645" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="645,645" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
        </screen>"""

    def __init__(self, session):
        print("[M3UConverter] ConversionSelector initialized")
        Screen.__init__(self, session)
        self.session = session
        self.skinName = "ConversionSelector"
        self.is_modal = True
        self.setTitle(title_plug)
        self.menu = [
            (_("M3U ➔ Enigma2 Bouquets"), "m3u_to_tv", "m3u"),
            (_("Enigma2 Bouquets ➔ M3U"), "tv_to_m3u", "tv"),
            (_("JSON ➔ Enigma2 Bouquets"), "json_to_tv", "json"),
            (_("XSPF ➔ M3U Playlist"), "xspf_to_m3u", "xspf"),
            (_("M3U ➔ JSON"), "m3u_to_json", "m3u"),
            (_("Remove M3U Bouquets"), "purge_m3u_bouquets", None)
        ]
        self["list"] = MenuList([(x[0], x[1]) for x in self.menu])
        self["Title"] = Label(title_plug)
        self["info"] = Label('')
        self["text"] = Label('')
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
        """Remove all bouquet files created by M3UConverter with correct pattern"""
        create_backup()
        removed_files = []

        # Remove matching bouquet files
        for f in listdir(directory):
            file_path = join(directory, f)
            if isfile(file_path) and f.endswith(pattern):
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
                    if not line.endswith(pattern + '"'):
                        f.write(line)

        if removed_files:
            message = "Removed %d bouquet(s):\n%s" % (
                len(removed_files),
                "\n".join(removed_files)
            )
        else:
            message = "No M3UConverter bouquets found to remove."

        self.session.open(MessageBox, message, MessageBox.TYPE_INFO, timeout=5)

    def selectionMade(self):
        selection = self["list"].getCurrent()
        if selection:
            conversion_type = selection[1]
            if conversion_type == "purge_m3u_bouquets":
                self.purge_m3u_bouquets()
            else:
                self.openFileBrowser(conversion_type)

    def open_file_browser(self, conversion_type):
        print(f"[DEBUG] Opening browser for {conversion_type}")
        patterns = {
            "m3u_to_tv": r"(?i)^.*\.(m3u|m3u8)$",
            "tv_to_m3u": r"(?i)^.*\.tv$",
            "json_to_tv": r"(?i)^.*\.json$",
            "xspf_to_m3u": r"(?i)^.*\.xspf$",
            "m3u_to_json": r"(?i)^.*\.(m3u|m3u8)$",
        }
        start_dir = "/media/hdd" if isdir("/media/hdd") else "/tmp"

        def callback(selected_file=None):
            if selected_file:
                self.fileSelected(selected_file, conversion_type)

        self.session.openWithCallback(
            callback,
            M3UFileBrowser,
            startdir=start_dir,
            matchingPattern=patterns.get(conversion_type, r".*"),
            conversion_type=conversion_type
        )

    def _get_browser_title(self, conversion_type):
        titles = {
            "m3u_to_tv": _("Select M3U file to convert"),
            "tv_to_m3u": _("Select Bouquet file to convert"),
            "json_to_tv": _("Select JSON file to convert"),
            "xspf_to_m3u": _("Select XSPF playlist to convert"),
            "m3u_to_json": _("Select M3U file to convert")
        }
        return titles.get(conversion_type, _("Select file"))

    def fileSelected(self, res, conversion_type):
        logger.debug(f"File selected callback. Result: {res}")
        try:
            if not res:
                return

            if not conversion_type:
                raise ValueError("Missing conversion type")

            logger.debug(f"Processing selected file: {res}")
            title_map = {
                "m3u_to_tv": _("M3U to Enigma2 Bouquet Conversion"),
                "tv_to_m3u": _("Enigma2 Bouquet to M3U Conversion"),
                "json_to_tv": _("JSON to Enigma2 Bouquet Conversion"),
                "xspf_to_m3u": _("XSPF to M3U Playlist Conversion"),
                "m3u_to_json": _("M3U to JSON Conversion")
            }

            # Open the converter directly and start the conversion
            converter = UniversalConverter(
                session=self.session,
                conversion_type=conversion_type,
                selected_file=res,
                auto_start=True
            )

            converter.setTitle(title_map.get(conversion_type, title_plug))
            self.session.open(converter)
        except Exception as e:
            logger.error(f"Error in fileSelected: {str(e)}")
            self.session.open(MessageBox, _("fileSelected Error: selection"), MessageBox.TYPE_ERROR, timeout=5)

    def convert_json_to_tv(self, filepath):
        converter = UniversalConverter(self.session, "json_to_tv")
        converter.selected_file = filepath
        converter.convert_json_to_tv()

    def convert_xspf_to_m3u(self, filepath):
        converter = UniversalConverter(self.session, "xspf_to_m3u")
        converter.selected_file = filepath
        converter.convert_xspf_to_m3u()

    def select_item(self):
        selection = self["list"].getCurrent()
        if not selection:
            return

        if selection[1] == "purge_m3u_bouquets":
            self.purge_m3u_bouquets()
            return

        self.session.open(
            UniversalConverter,
            conversion_type=selection[1],
            auto_open_browser=False
        )

    def show_pre_conversion_dialog(self):
        from Screens.ChoiceBox import ChoiceBox

        options = [
            (_("Proceed to file selection"), "open_browser"),
            (_("Show conversion info"), "show_info"),
            (_("Cancel"), "cancel")
        ]

        self.session.openWithCallback(
            self.handle_pre_conversion_choice,
            ChoiceBox,
            title=_("How do you want to proceed?"),
            list=options
        )

    def handle_pre_conversion_choice(self, choice):
        if choice and choice[1] == "open_browser":
            self.openFileBrowser(self.selected_conversion)
        elif choice and choice[1] == "show_info":
            self.show_conversion_info()
            self.show_pre_conversion_dialog()

    def show_conversion_info(self):
        """Show specific information for the conversion type"""
        info = {
            "m3u_to_tv": _("M3U to Enigma2 Bouquet Conversion"),
            "tv_to_m3u": _("Enigma2 Bouquet to M3U Conversion"),
            "json_to_tv": _("JSON to Enigma2 Bouquet Conversion"),
            "xspf_to_m3u": _("XSPF to M3U Playlist Conversion"),
            "m3u_to_json": _("M3U to JSON Conversion")
        }.get(self.selected_conversion, _("Conversion information"))

        self.session.open(
            MessageBox,
            info,
            MessageBox.TYPE_INFO,
            timeout=5
        )


class UniversalConverter(Screen):
    if screen_width > 1280:

        skin = """
        <screen name="UniversalConverter" position="center,center" size="1920,1080" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1920,1080" zPosition="-2" />
            <widget name="list" position="65,70" size="1122,797" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="65,920" size="1127,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="65,880" size="1125,30" backgroundColor="#05000603" transparent="1" foregroundColor="white" />
            <widget source="progress_text" render="Label" position="65,880" size="1124,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="blue" />
            <eLabel name="" position="1200,810" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1200,865" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1200,920" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="1220,125" size="640,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="1220,166" zPosition="20" size="640,360" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="65,1035" size="280,6" zPosition="12" />
            <widget name="key_red" position="65,990" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="65,990" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="365,1035" size="280,6" zPosition="12" />
            <widget name="key_green" position="365,990" size="280,45" zPosition="11" font="Regular; 30" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="364,990" size="280,45" zPosition="11" font="Regular; 30" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="666,1035" size="280,6" zPosition="12" />
            <widget name="key_yellow" position="664,990" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="664,990" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="968,1035" size="280,6" zPosition="12" />
            <widget name="key_blue" position="967,990" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="967,990" size="280,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    else:
        skin = """
        <screen name="UniversalConverter" position="center,center" size="1280,720" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="23,608" size="1185,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="25,582" size="1180,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <widget source="progress_text" render="Label" position="24,582" size="1180,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1111,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1165,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1220,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="25,700" size="250,6" zPosition="12" />
            <widget name="key_red" position="25,660" size="250,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="25,660" size="250,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="280,700" size="250,6" zPosition="12" />
            <widget name="key_green" position="280,660" size="250,45" zPosition="11" font="Regular; 30" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="280,660" size="250,45" zPosition="11" font="Regular; 30" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="541,700" size="250,6" zPosition="12" />
            <widget name="key_yellow" position="539,660" size="250,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="539,660" size="250,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="798,700" size="250,6" zPosition="12" />
            <widget name="key_blue" position="797,660" size="250,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="797,660" size="250,45" zPosition="11" font="Regular; 30" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    def __init__(self, session, conversion_type, selected_file=None, auto_open_browser=False, auto_start=False):
        Screen.__init__(self, session)
        self.setTitle(title_plug)
        self.conversion_type = conversion_type
        self.m3u_list = []
        self.bouquet_list = []
        self.converter = core_converter
        self.selected_file = selected_file
        self.auto_open_browser = auto_open_browser
        self.progress = None
        self.is_converting = False
        self.auto_start = auto_start
        base_path = config.plugins.m3uconverter.lastdir.value
        self.full_path = base_path
        self["list"] = MenuList([])
        self["Title"] = Label(title_plug)
        self["status"] = Label(_("Ready"))
        self["key_red"] = StaticText(_("Open File"))
        self["key_green"] = StaticText("")
        self["key_yellow"] = StaticText(_("Filter"))
        self["key_blue"] = StaticText(_("Tools"))
        self.progress_source = Progress()
        self["progress_source"] = self.progress_source
        self["progress_text"] = StaticText("")
        self["progress_source"].setValue(0)
        self.initialservice = self.session.nav.getCurrentlyPlayingServiceReference()
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MediaPlayerActions", "MenuActions"], {
            "red": self.open_file,
            "green": self.start_conversion,
            "yellow": self.toggle_filter,
            "blue": self.blue_button_action,
            "menu": self.open_settings,
            "ok": self.key_ok,
            "cancel": self.keyClose,
            "stop": self.stop_player
        }, -1)

        self["status"] = Label(_("Ready: Select the file from the %s you configured in settings.") % self.full_path)

        self.file_loaded = False if selected_file is None else True

        if self.conversion_type == "tv_to_m3u":
            self.init_tv_converter()

        if auto_open_browser and not selected_file:
            self.onFirstExecBegin.append(self.open_file)

        if self.auto_start and self.selected_file:
            self.onShown.append(self.start_conversion_after_show)

    def blue_button_action(self):
        """Gestione dinamica del tasto blue in base allo stato"""
        if self.is_converting:
            self.cancel_convert()
        else:
            self.show_tools_menu()

    def start_conversion_after_show(self):
        """Avvia la conversione automaticamente dopo che la schermata è stata mostrata"""
        try:
            self.onShown.remove(self.start_conversion_after_show)
            if self.auto_start and self.selected_file:
                # Please wait a moment to allow the UI to stabilize
                self.start_timer = eTimer()
                self.start_timer.callback.append(self.delayed_start)
                self.start_timer.start(1000)  # 1 second delay
        except:
            pass

    def delayed_start(self):
        """Avvia la conversione con un leggero ritardo"""
        try:
            self.start_timer.stop()
            self.start_conversion()
        except Exception as e:
            logger.error(f"Error in delayed_start: {str(e)}")

    # optional
    def create_manual_backup(self):
        try:
            self.converter._create_backup()
            self.session.open(MessageBox, _("Backup created successfully!"), MessageBox.TYPE_INFO, timeout=5)
        except Exception as e:
            self.session.open(MessageBox, _(f"Backup failed: {str(e)}"), MessageBox.TYPE_ERROR, timeout=5)

    def init_m3u_converter(self):
        self.onShown.append(self.delayed_file_browser)

    def delayed_file_browser(self):
        try:
            self.onShown.remove(self.delayed_file_browser)
        except ValueError:
            pass

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
            self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR, timeout=5)

    def open_file(self):
        """The ONLY way to manage file browser opening"""
        logger.debug(f"Opening file browser for {self.conversion_type}")

        try:
            path = "/etc/enigma2" if self.conversion_type == "tv_to_m3u" else config.plugins.m3uconverter.lastdir.value
            pattern = r"(?i)^.*\.tv$" if self.conversion_type == "tv_to_m3u" else r"(?i)^.*\.(m3u|m3u8)$"

            if not path or not isdir(path):
                path = "/media/hdd" if isdir("/media/hdd") else "/tmp"

            self.session.openWithCallback(
                self.file_selected,
                M3UFileBrowser,
                startdir=path,
                matchingPattern=pattern,
                conversion_type=self.conversion_type
            )

        except Exception as e:
            logger.error(f"Error opening file browser: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error opening file browser:\n%s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=5
            )

    def toggle_filter(self):
        """Enable/disable filtering of non-working channels"""
        if not hasattr(self, 'selected_file') or not self.selected_file:
            self["status"].setText(_("No file loaded to filter"))
            return

        config.plugins.m3uconverter.filter_dead_channels.value = \
            not config.plugins.m3uconverter.filter_dead_channels.value
        config.plugins.m3uconverter.filter_dead_channels.save()

        self.parse_m3u(self.selected_file)

        # Save previous status
        self._previous_status = self["status"].getText()

        # Show temporary filter status
        status = _("Filter: %s") % (_("ON") if config.plugins.m3uconverter.filter_dead_channels.value else _("OFF"))
        self["status"].setText(status)

        # Restore previous status after 6 seconds
        self.status_timer = eTimer()
        self.status_timer.callback.append(self._restore_status_text)
        self.status_timer.start(6000)  # milliseconds

    def _restore_status_text(self):
        """Restore the previous status text"""
        if hasattr(self, "_previous_status"):
            self["status"].setText(self._previous_status)

        if hasattr(self, "status_timer"):
            self.status_timer.stop()
            self.status_timer.callback.remove(self._restore_status_text)

    def show_tools_menu(self):
        """Additional tools menu"""
        from Screens.ChoiceBox import ChoiceBox
        menu = [
            (_("Create Backup"), "backup"),
            (_("Reload Services"), "reload"),
            (_("Plugin Info"), "info")
        ]

        def tool_selected(choice):
            if choice:
                if choice[1] == "backup":
                    self.create_manual_backup()
                elif choice[1] == "reload":
                    reload_services()
                    self["status"].setText(_("Services reloaded"))
                elif choice[1] == "info":
                    self.show_plugin_info()

        self.session.openWithCallback(
            tool_selected,
            ChoiceBox,
            title=_("Tools Menu"),
            list=menu
        )

    def _safe_show_summary(self):
        """Wrapper sicuro per show_summary con gestione errori"""
        logger.debug("Preparing conversion summary...")
        try:
            if not self.m3u_list:
                self["status"].setText(_("No channels loaded!"))
                return

            if hasattr(self.converter, 'filter_channels'):
                valid_channels = self.converter.filter_channels(
                    self.m3u_list,
                    "working" if config.plugins.m3uconverter.filter_dead_channels.value else "all"
                )
            else:
                valid_channels = self.m3u_list

            self.show_summary(valid_channels)
        except Exception as e:
            logger.error(f"Show summary failed: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error preparing summary:\n%s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=5
            )

    def show_summary(self, channels):
        """Show summary with valid channels"""
        try:
            summary = [
                _("=== Conversion Summary ==="),
                _("Total channels: %d") % len(channels),
                _("Valid channels: %d") % len([c for c in channels if c.get('url')]),
                _("Groups detected: %d") % len(set(c.get('group', '') for c in channels)),
                "",
                _("Press OK to confirm or Cancel to abort.")
            ]

            # Show preview of first 5 channels
            sample = [c['name'] for c in channels[:5]]
            if len(channels) > 5:
                sample.append("...")
            summary.extend(["", _("Sample channels:")] + sample)

            self.session.openWithCallback(
                self._start_conversion_if_confirmed,
                MessageBox,
                "\n".join(summary),
                MessageBox.TYPE_YESNO,
                timeout=5
            )
        except Exception as e:
            raise RuntimeError(_("Summary generation error: %s") % str(e))

    def _start_conversion_if_confirmed(self, confirmed):
        if confirmed:
            self.start_conversion()

    def parse_file(self):
        """Main router for the different parsers"""
        if not self.selected_file:
            self.open_file()
            return

        ext = self.selected_file.lower().split('.')[-1]

        if ext in ('m3u', 'm3u8'):
            self.parse_m3u()
        elif ext == 'json':
            self.parse_json()
        elif ext == 'xspf':
            self.parse_xspf()
        else:
            self.show_error(_("Unsupported file type"))

    def start_conversion(self):
        if self.is_converting:
            return

        if not hasattr(self, 'selected_file') or not self.selected_file:
            self.session.open(MessageBox, _("No file selected for conversion"), MessageBox.TYPE_WARNING)
            return

        # Add this condition for the new conversion type
        if self.conversion_type == "m3u_to_json":
            if not self.m3u_list:
                self.parse_m3u(self.selected_file)
            self.convert_m3u_to_json()
            return

    # def start_conversion(self):
        # if self.is_converting:
            # return

        # if not hasattr(self, 'selected_file') or not self.selected_file:
            # self.session.open(MessageBox, _("No file selected for conversion"), MessageBox.TYPE_WARNING)
            # return

        # Show confirmation message box
        message = _("Are you sure you want to convert this file?") + "\n" + self.selected_file
        self.session.openWithCallback(self.confirmationCallback, MessageBox, message, MessageBox.TYPE_YESNO)

    def confirmationCallback(self, confirmed):
        if confirmed:
            try:
                # Disable buttons during conversion
                self.is_converting = True
                self.cancel_conversion = False
                self["key_green"].setText(_("Converting..."))
                self["key_blue"].setText(_("Cancel"))

                # Start conversion in a separate thread
                import threading
                self.conversion_thread = threading.Thread(
                    target=self._convert_thread,
                    args=(self.selected_file, self.conversion_type)
                )
                self.conversion_thread.start()

            except Exception as e:
                self.is_converting = False
                self["key_green"].setText(_("Convert"))
                self["key_blue"].setText(_("Tools"))
                logger.error(f"Error starting conversion: {str(e)}")
                self.session.open(MessageBox, _("Error starting conversion"), MessageBox.TYPE_ERROR)

    def update_path(self):
        """Update path with all possible device locations"""
        try:
            if self.conversion_type == "tv_to_m3u":
                self.full_path = "/etc/enigma2"
                if not isdir(self.full_path):
                    self.full_path = "/tmp"
                return

            potential_paths = [
                config.plugins.m3uconverter.lastdir.value,
                "/media/hdd/movie",
                "/media/hdd",
                "/media/usb/movie",
                "/media/usb",
                "/media/nas/movie",
                "/media/nas",
                "/media/ba/movie",
                "/media/ba",
                "/media/net/movie",
                "/media/net",
                "/hdd/movie",
                "/hdd",
                "/usb",
                "/autofs/movie",
                "/autofs",
                "/tmp"
            ]

            for path in potential_paths:
                if path and isdir(path):
                    self.full_path = path
                    break
            else:
                self.full_path = "/tmp"  # Fallback

            if self.full_path.split('/')[-1] != "movie" and self.full_path != "/tmp":
                movie_path = join(self.full_path, "movie")
                if not isdir(movie_path):
                    makedirs(movie_path, exist_ok=True)
                    chmod(movie_path, 0o755)
                self.full_path = movie_path

            logger.info(f"Selected storage path: {self.full_path}")

        except Exception as e:
            logger.error(f"Path update failed: {str(e)}")
            self.full_path = "/tmp"

    def file_selected(self, res=None):
        """Bulletproof file selection handler"""
        if not res or not isinstance(res, (str, list, tuple)):
            self["status"].setText(_("Invalid selection"))
            return
        try:
            # DEBUG START
            logger.debug("=== FILE SELECTION STARTED ===")

            # Reset all states
            self.file_loaded = False
            self.m3u_list = []
            # self.update_green_button()
            self["status"].setText(_("Processing selection..."))

            # Validate input
            if not res:
                logger.debug("No file selected")
                self["status"].setText(_("No file selected"))
                return

            # Get normalized path
            try:
                if isinstance(res, (tuple, list)) and res:
                    selected_path = normpath(str(res[0]))
                else:
                    selected_path = normpath(str(res))

                if not exists(selected_path):
                    raise IOError(_("File not found"))
            except Exception as e:
                logger.error(f"Path error: {str(e)}")
                self.show_error(str(e))
                return

            # DEBUG: Before processing
            logger.debug(f"Processing file: {selected_path}")

            # Process file
            path = res[0] if isinstance(res, (list, tuple)) else res
            self.selected_file = normpath(str(path))
            try:
                if self.conversion_type == "m3u_to_tv":
                    self.parse_m3u(selected_path)
                elif self.conversion_type == "tv_to_m3u":
                    self.parse_tv(selected_path)

                # Update states
                self.file_loaded = True
                logger.debug(f"File loaded successfully, channels: {len(self.m3u_list)}")
            except Exception as e:
                logger.error(f"Processing failed: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"File selection failed: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            self.show_error(str(e))
        finally:
            logger.debug("=== FILE SELECTION COMPLETE ===")

    def _update_ui_success(self, channel_count):
        self["status"].setText(_("Loaded %d channels. Are you ready to convert? Press Green to proceed.") % channel_count)
        if hasattr(self, 'instance'):
            self.instance.invalidate()

    def _show_parsing_error(self, error):
        self.session.open(
            MessageBox,
            _("Invalid file format:\n%s") % str(error),
            MessageBox.TYPE_ERROR,
            timeout=5
        )
        self["status"].setText(_("Error loading file"))

    def update_channel_list(self):
        display_list = []
        for idx, channel in enumerate(self.m3u_list[:200]):  # Show max 200 items
            name = sub(r'\[.*?\]', '', channel.get('name', '')).strip()
            group = channel.get('group', '')
            display_list.append(f"{idx + 1: 03d}. {group + ' - ' if group else ''}{name}")

        self["list"].setList(display_list)
        self["status"].setText(_("Loaded %d channels") % len(self.m3u_list))

    def process_url(self, url):
        """Process URLs based on settings"""
        url = url.replace(":", "%3a")
        if config.plugins.m3uconverter.hls_convert.value:
            if any(url.lower().endswith(x) for x in ('.m3u8', '.stream')):
                url = f"hls://{url}"
        return url

    def convert_hls(self, url):
        """Convert HLS URLs"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.path.lower().endswith(('.m3u8', '.stream')):
            return f"hls://{url}"
        return url

    def _convert_thread(self, m3u_path, conversion_type):
        """Threaded conversion to avoid blocking the UI"""
        try:
            def progress_callback(progress, text):
                if hasattr(self, 'cancel_conversion') and self.cancel_conversion:
                    raise Exception("Conversion cancelled by user")

                from twisted.internet import reactor
                reactor.callFromThread(self._update_progress_ui, progress, text)

            # Perform conversion based on type
            if conversion_type == "m3u_to_tv":
                result = self._convert_m3u_to_tv_task(m3u_path, progress_callback)
            elif conversion_type == "tv_to_m3u":
                result = self._convert_tv_to_m3u_task(m3u_path, progress_callback)
            else:
                result = (False, "Unsupported conversion type")

            # Update UI from the main thread
            from twisted.internet import reactor
            if self.cancel_conversion:
                reactor.callFromThread(self._conversion_cancelled)
            else:
                reactor.callFromThread(self._conversion_finished, result)

        except Exception as e:
            from twisted.internet import reactor
            if "cancelled" in str(e).lower():
                reactor.callFromThread(self._conversion_cancelled)
            else:
                reactor.callFromThread(self._conversion_error, str(e))

    def _convert_m3u_to_tv_task(self, m3u_path, progress_callback):
        """Task for converting M3U to TV bouquet"""
        try:
            # Initial cancellation check
            if self.cancel_conversion:
                return (False, "Conversion cancelled")
                
            # Initialize EPG mapper
            epg_mapper = EPGServiceMapper(prefer_satellite=True)
            if not epg_mapper.initialize():
                logger.warning("EPGServiceMapper failed to initialize, continuing without EPG support")
            
            # Extract EPG URL from the M3U file
            epg_url = epg_mapper.extract_epg_url_from_m3u(m3u_path)
            
            # Read the M3U file
            with open(m3u_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Parse M3U content
            self.m3u_list = self._parse_m3u_content(content)

            # Normalize fields for consistency
            normalized_list = []
            for ch in self.m3u_list:
                normalized_list.append({
                    'name': ch.get('title', ''),          # use title as name
                    'url': ch.get('uri', ''),             # use uri as url
                    'group': ch.get('group-title', ''),   # optional
                    'tvg_id': ch.get('tvg-id', ''),
                    'tvg_name': ch.get('tvg-name', ''),
                    'logo': ch.get('tvg-logo', ''),
                    'user_agent': ch.get('user_agent', ''),
                    'program_id': ch.get('program-id', '')
                })
            self.m3u_list = normalized_list

            # Apply binary data filter
            original_count = len(self.m3u_list)
            self.m3u_list = self.filter_binary_data(self.m3u_list)
            filtered_count = original_count - len(self.m3u_list)
            
            if filtered_count > 0:
                logger.warning("Filtered out %d channels with binary data", filtered_count)
            
            total_channels = len(self.m3u_list)
            
            # If there are no valid channels, stop
            if total_channels == 0:
                return (False, "No valid channels found after filtering")

            groups = {}
            epg_data = []

            # Phase 1: Group channels with EPG mapping
            for idx, channel in enumerate(self.m3u_list):
                # Periodic cancellation check
                if self.cancel_conversion:
                    return (False, "Conversion cancelled")
                    
                progress = (idx + 1) / total_channels * 100
                progress_callback(progress, _("Processing: %s") % channel.get('name', 'Unknown'))
                
                # Find best matching service
                clean_name = epg_mapper.clean_channel_name(channel['name'])
                sref, match_type = epg_mapper.find_best_service_match(clean_name, channel.get('tvg_id'))
                
                if sref:
                    # Use hybrid service reference for bouquet
                    hybrid_sref = epg_mapper.generate_hybrid_sref(sref, channel['url'])
                    # For EPG, we need DVB service reference (without URL)
                    epg_sref = epg_mapper.generate_hybrid_sref(sref, for_epg=True)
                    
                    # Add to EPG data
                    epg_data.append({
                        'tvg_id': channel.get('tvg_id', channel['name']),
                        'sref': epg_sref,
                        'name': channel['name']
                    })
                    
                    # Use hybrid reference for bouquet
                    channel['url'] = hybrid_sref
                else:
                    # Fallback to standard IPTV service reference
                    channel['url'] = epg_mapper.generate_service_reference(channel['url'])

                group = channel.get('group', 'Default')
                groups.setdefault(group, []).append(channel)

            # Phase 2: Write bouquet(s)
            bouquet_name = self.get_safe_filename(basename(m3u_path).split('.')[0])
            
            if config.plugins.m3uconverter.bouquet_mode.value == "single":
                # Create one bouquet with all channels
                all_channels = []
                for group_channels in groups.values():
                    all_channels.extend(group_channels)
                
                self.write_group_bouquet(bouquet_name, all_channels)
            else:
                # Create separate bouquets by group
                for group_idx, (group, channels) in enumerate(groups.items()):
                    safe_group_name = self.get_safe_filename(group)
                    self.write_group_bouquet(safe_group_name, channels)

            # Phase 3: Update main bouquet
            if config.plugins.m3uconverter.bouquet_mode.value == "single":
                self.update_main_bouquet([bouquet_name])
            else:
                self.update_main_bouquet([self.get_safe_filename(group) for group in groups.keys()])
            
            # Phase 4: Generate EPG files if data is available
            if epg_data and config.plugins.m3uconverter.epg_enabled.value:
                epg_mapper.generate_epg_channels_file(epg_data, bouquet_name)
                epg_mapper.generate_epg_sources_file(bouquet_name, epg_url)

            return (True, total_channels, len(epg_data))

        except Exception as e:
            logger.error(f"Error in M3U to TV conversion: {str(e)}")
            return (False, str(e))

    def _convert_tv_to_m3u_task(self, tv_path, progress_callback):
        """Task specifico per conversione TV to M3U"""
        try:
            with open(tv_path, 'r', encoding='utf-8') as f:
                content = f.read()

            pattern = r'#SERVICE 4097:0:1:0:0:0:0:0:0:0:(.*?)\n#DESCRIPTION (.*?)\n'
            matches = findall(pattern, content, DOTALL)

            total_channels = len(matches)
            output_file = join(dirname(tv_path), f"{basename(tv_path).split('.')[0]}.m3u")

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('#EXTM3U\n')
                for idx, (url, description) in enumerate(matches):
                    if self.cancel_conversion:
                        return (False, "Conversion cancelled")

                    progress = (idx + 1) / total_channels * 100
                    progress_callback(progress, _("Processing: %s") % description)

                    decoded_url = unquote(url)
                    f.write(f'#EXTINF:-1,{description}\n')
                    f.write(f'{decoded_url}\n')

            return (True, total_channels, output_file)

        except Exception as e:
            logger.error(f"Error in TV to M3U conversion: {str(e)}")
            return (False, str(e))

    def convert_m3u_to_json(self):
        """Convert M3U playlist to JSON format"""
        def _m3u_to_json_conversion():
            try:
                if not self.m3u_list:
                    raise ValueError("No M3U data to convert")

                # Create JSON structure
                json_data = {"playlist": []}
                
                for channel in self.m3u_list:
                    channel_data = {}
                    
                    # Add all available attributes
                    if channel.get('tvg_id'):
                        channel_data["tvg-id"] = channel['tvg_id']
                    if channel.get('tvg_name'):
                        channel_data["tvg-name"] = channel['tvg_name']
                    if channel.get('logo'):
                        channel_data["tvg-logo"] = channel['logo']
                    if channel.get('group'):
                        channel_data["group-title"] = channel['group']
                    if channel.get('name'):
                        channel_data["title"] = channel['name']
                    if channel.get('url'):
                        channel_data["url"] = channel['url']
                    if channel.get('duration'):
                        channel_data["duration"] = channel['duration']
                    if channel.get('user_agent'):
                        channel_data["user-agent"] = channel['user_agent']
                    if channel.get('program_id'):
                        channel_data["program-id"] = channel['program_id']
                    
                    json_data["playlist"].append(channel_data)

                # Generate output filename
                base_name = basename(self.selected_file).split('.')[0]
                output_file = join(dirname(self.selected_file), f"{base_name}.json")
                
                # Write JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=4, ensure_ascii=False)
                
                return (True, output_file, len(json_data["playlist"]))
                
            except Exception as e:
                logger.error(f"Error converting M3U to JSON: {str(e)}")
                return (False, str(e))

        # Run conversion in thread
        def conversion_task():
            try:
                return _m3u_to_json_conversion()
            except Exception as e:
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

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
        with improved encoding handling.
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
            markera = "#SERVICE 1:64:0:0:0:0:0:0:0:0::--- | Archimede Converter | ---"
            markerb = "#DESCRIPTION --- | Archimede Converter | ---"

            with open(temp_file, "w", encoding="utf-8") as f:  # Cambiato a UTF-8
                f.write("#NAME " + name_bouquet + "\n")
                f.write(markera + "\n")
                f.write(markerb + "\n")
                for idx, ch in enumerate(channels, 1):
                    # Use the URL directly without further processing
                    f.write("#SERVICE " + ch["url"] + "\n")

                    # Clean and transliterate the description
                    desc = ch["name"]
                    # Remove non-printable characters
                    desc = ''.join(c for c in desc if c.isprintable() or c.isspace())
                    # Transliterate if necessary
                    desc = transliterate(desc)
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

    def get_safe_filename(self, name):
        """Generate a secure file name for bouquets without duplicate suffixes"""
        # Remove any existing suffixes before processing
        if name.endswith('_m3ubouquet'):
            name = name[:-11]  # Remove "_m3ubouquet"
        
        normalized = unicodedata.normalize("NFKD", name)
        safe_name = normalized.encode('ascii', 'ignore').decode('ascii')
        safe_name = sub(r'[^a-z0-9_-]', '_', safe_name.lower())
        safe_name = sub(r'_+', '_', safe_name).strip('_')

        # Add the suffix only once
        suffix = "_m3ubouquet"
        base_name = safe_name[:50 - len(suffix)] if len(safe_name) > 50 - len(suffix) else safe_name

        return base_name + suffix if base_name else "m3uconverter_bouquet"

    def _get_output_filename(self):
        """Generate a unique file name for export"""
        timestamp = strftime("%Y%m%d_%H%M%S")
        return f"{archimede_converter_path}/archimede_export_{timestamp}.m3u"

    def parse_m3u(self, filename=None):
        """M3U parsing with detailed debugging"""
        logger.debug("Starting M3U parsing")
        try:
            file_to_parse = filename or self.selected_file
            if not file_to_parse:
                raise ValueError(_("No file specified"))

            logger.info(f"Starting M3U parsing: {file_to_parse}")
            # Backup
            if config.plugins.m3uconverter.backup_enable.value:
                create_backup()

            with open(file_to_parse, 'r', encoding='utf-8', errors='replace') as f:
                data = f.read()

            # Reset
            self.m3u_list = []
            self.filename = file_to_parse

            channels = self._parse_m3u_content(data)

            for channel in channels:
                if not channel.get('uri'):
                    continue

                self.m3u_list.append({
                    'name': channel.get('title', ''),
                    'group': channel.get('group-title', ''),
                    'tvg_name': channel.get('tvg-name', ''),
                    'logo': channel.get('tvg-logo', ''),
                    'url': self.process_url(channel['uri']),
                    'duration': channel.get('length', ''),
                    'user_agent': channel.get('user_agent', ''),
                    'program_id': channel.get('program-id', '')
                })

            display_list = []
            for channel in self.m3u_list:
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', '')
                display_list.append(f"{group + ' - ' if group else ''}{name}")

            self["list"].setList(display_list)
            logger.debug(f"Parsing complete, found {len(self.m3u_list)} channels")
            # self.update_channel_list()
            # self.m3u_list = channels
            # self["list"].setList([c[0] for c in channels])
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_list))
            self["key_green"].setText(_("Convert to M3U"))
        except Exception as e:
            logger.error(f"Error parsing M3U: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            raise

    def _parse_m3u_content(self, data):
        """Advanced parser for M3U content"""

        def is_textual_data(text):
            """Check if data is likely textual (not binary)"""
            if not text:
                return False
                
            # Check for common binary data patterns (removed binary_indicators unused)
            text = str(text)
            # If text is too long, it's likely binary
            if len(text) > 200:
                return False
                
            # Check for high percentage of non-printable characters
            printable_count = sum(1 for c in text if c.isprintable() or c.isspace())
            if printable_count / len(text) < 0.7:  # Less than 70% printable
                return False
                
            return True

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
                    title = parts[1].strip()
                    # Filter non-text data
                    if not is_textual_data(title):
                        logger.warning("Skipping non-textual channel name: %s", title[:50] + "..." if len(title) > 50 else title)
                        current_params = {}
                        continue
                    current_params['title'] = title
                attribs = get_attributes(parts[0], first_key_as_length=True)
                current_params.update(attribs)

            elif line.startswith('#EXTGRP:'):
                group_value = line[8:].strip()
                if is_textual_data(group_value):
                    current_params['group-title'] = group_value
                else:
                    logger.warning("Skipping non-textual group title")

            elif line.startswith('#EXTVLCOPT:'):
                opts = line[11:].split('=', 1)
                if len(opts) == 2:
                    key = opts[0].lower().strip()
                    value = opts[1].strip()
                    if is_textual_data(value):
                        if key == 'http-user-agent':
                            current_params['user_agent'] = value
                        elif key == 'program':
                            current_params['program-id'] = value

            elif line.startswith('#'):
                continue

            else:
                if current_params.get('title'):
                    # Filtra URL non validi
                    if line.startswith(('http://', 'https://', 'rtsp://', 'rtmp://', 'udp://', 'rtp://')):
                        current_params['uri'] = line.strip()
                        entries.append(current_params)
                    else:
                        logger.warning("Skipping non-HTTP URL: %s", line[:50] + "..." if len(line) > 50 else line)
                    current_params = {}

        return entries

    def filter_binary_data(self, channels):
        """Filter out channels with binary data in names or URLs"""
        filtered_channels = []

        for channel in channels:
            # Check channel name
            name = channel.get('name', '')
            if not self._is_valid_text(name):
                logger.warning("Skipping channel with binary name: %s", name[:50] + "..." if len(name) > 50 else name)
                continue

            # Check group name
            group = channel.get('group', '')
            if group and not self._is_valid_text(group):
                logger.warning("Skipping channel with binary group: %s", group[:50] + "..." if len(group) > 50 else group)
                continue

            # Check URL
            url = channel.get('url', '')
            if not url.startswith(('http://', 'https://', 'rtsp://', 'rtmp://', 'udp://', 'rtp://', '4097:')):
                logger.warning("Skipping channel with invalid URL: %s", url[:50] + "..." if len(url) > 50 else url)
                continue

            filtered_channels.append(channel)

        return filtered_channels

    def _is_valid_text(self, text):
        """Check if text is valid (not binary data)"""
        if not text or not isinstance(text, str):
            return False
            
        # Check for common binary patterns (rimosso binary_patterns non utilizzato)
        text_str = str(text)
        
        # Length check
        if len(text_str) > 200:
            return False
            
        # Printable characters check
        printable_count = sum(1 for c in text_str if c.isprintable() or c.isspace())
        if printable_count / len(text_str) < 0.7:  # Less than 70% printable
            return False
            
        return True

    # TV to M3U Conversion Methods
    def parse_tv(self, filename=None):
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

            # self.update_channel_list()
            self.m3u_list = channels
            self["list"].setList([c[0] for c in channels])
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_list))
            self["key_green"].setText(_("Convert to Bouquet"))
        except Exception as e:
            logger.error(f"Error parsing BOUQUET: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            raise

    def parse_json(self, filename=None):
        file_to_parse = filename or self.selected_file
        try:
            with open(file_to_parse, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.m3u_list = []
            for group in data.get('groups', []):
                for channel in group.get('channels', []):
                    self.m3u_list.append({
                        'name': channel.get('name', ''),
                        'url': channel.get('url', ''),
                        'group': group.get('name', ''),
                        'logo': channel.get('logo', '')
                    })

            self.update_channel_list()
            # self.m3u_list = channels
            # self["list"].setList([c[0] for c in channels])
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_list))
            self["key_green"].setText(_("Convert to JSON"))
        except Exception as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            raise

    def parse_xspf(self, filename=None):
        file_to_parse = filename or self.selected_file
        try:
            from xml.etree import ElementTree as ET
            tree = ET.parse(file_to_parse)
            root = tree.getroot()
            ns = {'ns': 'http://xspf.org/ns/0/'}

            self.m3u_list = []
            for track in root.findall('.//ns:track', ns):
                name = track.find('ns:title', ns)
                url = track.find('ns:location', ns)
                if name is not None and url is not None:
                    self.m3u_list.append({
                        'name': name.text,
                        'url': url.text,
                        'group': 'XSPF Import'
                    })

            self.update_channel_list()
            # self.m3u_list = channels
            # self["list"].setList([c[0] for c in channels])
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_list))
            self["key_green"].setText(_("Convert to XSPF"))
        except Exception as e:
            logger.error(f"Error parsing XSPF: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            raise

    def convert_json_to_tv(self):

        def _json_conversion_task():
            try:
                with open(self.selected_file, 'r') as f:
                    data = json.load(f)

                channels = []
                for group in data.get('groups', []):
                    for channel in group.get('channels', []):
                        channels.append({
                            'name': channel.get('name', ''),
                            'url': channel.get('url', ''),
                            'group': group.get('name', '')
                        })

                self.m3u_list = channels
                return self.convert_m3u_to_tv()

            except json.JSONDecodeError:
                raise RuntimeError(_("Invalid JSON file"))
            except Exception as e:
                raise RuntimeError(_("JSON processing error: %s") % str(e))

        threads.deferToThread(
            self.converter.safe_convert(_json_conversion_task)
        ).addBoth(self.conversion_finished)

    def convert_m3u_to_tv(self):
        def _real_conversion_task():
            try:
                # Initialize EPG service mapper
                epg_mapper = EPGServiceMapper(prefer_satellite=True)
                if not epg_mapper.initialize():
                    logger.warning("EPGServiceMapper failed to initialize, continuing without EPG support")

                # Extract EPG URL from M3U file
                epg_url = epg_mapper.extract_epg_url_from_m3u(self.selected_file)
                logger.debug("Extracted EPG URL: %s", epg_url)

                groups = {}
                epg_data = []
                total_channels = len(self.m3u_list)

                # Phase 1: Channel Grouping with EPG mapping
                for idx, channel in enumerate(self.m3u_list):
                    if not channel.get('url'):  # Skip channels without URLs
                        continue

                    clean_name = epg_mapper.clean_channel_name(channel['name'])
                    tvg_id = channel.get('tvg_id')
                    logger.debug("Processing channel: %s (tvg_id: %s)", channel['name'], tvg_id)

                    # Get the best service reference for EPG
                    sref, match_type = epg_mapper.find_best_service_match(clean_name, tvg_id)

                    if sref:
                        logger.debug("Found service reference for %s: %s (match type: %s)", channel['name'], sref, match_type)
                        # Use hybrid service reference for bouquet
                        hybrid_sref = epg_mapper.generate_hybrid_sref(sref, channel['url'])
                        # For EPG, we need the DVB service reference (without URL)
                        epg_sref = epg_mapper.generate_hybrid_sref(sref, for_epg=True)

                        # Add to EPG data
                        epg_data.append({
                            'tvg_id': tvg_id or channel['name'],
                            'sref': epg_sref,
                            'name': channel['name']
                        })

                        # Use the hybrid reference for the bouquet
                        channel['url'] = hybrid_sref
                    else:
                        logger.debug("No service reference found for %s, using standard IPTV reference", channel['name'])
                        # Fallback to standard IPTV reference
                        channel['url'] = epg_mapper.generate_service_reference(channel['url'])

                    logger.debug("Original URL: %s", channel['url'])
                    logger.debug("Generated service reference for bouquet: %s", hybrid_sref)
                    logger.debug("Generated service reference for EPG: %s", epg_sref)

                    group = channel.get('group', 'Default')
                    groups.setdefault(group, []).append(channel)
                    progress = (idx + 1) / total_channels * 100
                    name = str(channel.get("name") or "--")
                    self.update_progress(idx + 1, _("Processing: %s (%d%%)") % (name, progress))

                # Phase 2: Writing bouquets
                if config.plugins.m3uconverter.bouquet_mode.value == "single":
                    # Create a single bouquet with all channels
                    all_channels = []
                    for group_channels in groups.values():
                        all_channels.extend(group_channels)

                    bouquet_name = self.get_safe_filename(basename(self.selected_file).split('.')[0])
                    self.write_group_bouquet(bouquet_name, all_channels)
                    self.update_progress(
                        total_channels + 1,
                        _("Creating single bouquet: %s") % bouquet_name
                    )
                else:
                    # Create separate bouquets for each group
                    for group_idx, (group, channels) in enumerate(groups.items()):
                        self.write_group_bouquet(group, channels)
                        self.update_progress(
                            total_channels + group_idx,
                            _("Creating bouquet: %s") % group
                        )

                # Phase 3: Main bouquet update
                if config.plugins.m3uconverter.bouquet_mode.value == "single":
                    self.update_main_bouquet([bouquet_name])
                else:
                    self.update_main_bouquet(groups.keys())

                # Phase 4: Generate EPG files if we have EPG data
                if epg_data and config.plugins.m3uconverter.epg_enabled.value:
                    if config.plugins.m3uconverter.bouquet_mode.value == "single":
                        bouquet_name_for_epg = bouquet_name
                    else:
                        bouquet_name_for_epg = "all_groups"  # Usa un nome unico per EPG multi-bouquet

                    logger.debug("Generating EPG files for bouquet: %s", bouquet_name_for_epg)
                    logger.debug("EPG data for %d channels", len(epg_data))

                    # Generate channels.xml
                    channels_success = epg_mapper.generate_epg_channels_file(epg_data, bouquet_name_for_epg)
                    if not channels_success:
                        logger.error("Failed to generate EPG channels file")

                    # Generate sources.xml
                    sources_success = epg_mapper.generate_epg_sources_file(bouquet_name_for_epg, epg_url)
                    if not sources_success:
                        logger.error("Failed to generate EPG sources file")

                    if channels_success and sources_success:
                        logger.info("Generated EPG files for %d channels", len(epg_data))
                    else:
                        logger.warning("Failed to generate some EPG files")
                else:
                    logger.warning("No EPG data to generate files or EPG disabled")

                return (True, total_channels, len(epg_data) if epg_data else 0)
            except Exception as e:
                logger.exception(f"Error during real conversion {str(e)}")
                raise

        def conversion_task():
            try:
                return self.converter.safe_convert(_real_conversion_task)
            except Exception as e:
                logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def convert_tv_to_m3u(self):

        def _real_tv_to_m3u_conversion():
            try:
                output_file = self._get_output_filename()
                total_items = len(self.m3u_list)

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {currversion}\n')

                    for idx, (name, url) in enumerate(self.m3u_list):
                        f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                        f.write(f'{url}\n')

                        if idx % 10 == 0:
                            progress = (idx + 1) / total_items * 100
                            self.update_progress(
                                idx + 1,
                                _("Exporting: %s (%d%%)") % (name, int(progress))
                            )

                return (True, output_file)
            except IOError as e:
                raise RuntimeError(_("File write error: %s") % str(e))
            except Exception as e:
                raise RuntimeError(_("tv_to_m3u Conversion error: %s") % str(e))

        def conversion_task():
            try:
                return self.converter.safe_convert(_real_tv_to_m3u_conversion)
            except Exception as e:
                logger.error(f"TV-to-M3U conversion failed: {str(e)}")
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def convert_xspf_to_m3u(self):

        def _xspf_conversion():
            try:
                from xml.etree import ElementTree as ET
                tree = ET.parse(self.selected_file)
                root = tree.getroot()

                output_file = self._get_output_filename()
                ns = {'ns': 'http://xspf.org/ns/0/'}

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')

                    for track in root.findall('.//ns:track', ns):
                        title = track.find('ns:title', ns)
                        location = track.find('ns:location', ns)

                        if title is not None and location is not None:
                            f.write(f'#EXTINF:-1,{title.text}\n')
                            f.write(f'{location.text}\n')

                return (True, output_file)
            except ET.ParseError:
                raise RuntimeError(_("Invalid XSPF file"))
            except Exception as e:
                raise RuntimeError(_("XSPF conversion error: %s") % str(e))

        threads.deferToThread(
            self.converter.safe_convert(_xspf_conversion)
        ).addBoth(self.conversion_finished)

    def _conversion_finished(self, result):
        """Handle conversion finished"""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))

        if result[0]:  # Success
            if len(result) == 3:
                total_channels, epg_channels = result[1], result[2]
                msg = _("Successfully converted %d channels") % total_channels
                if epg_channels > 0:
                    msg += _("\nEPG mapped for %d channels") % epg_channels
            else:
                msg = _("Conversion completed successfully")

            self.session.open(MessageBox, msg, MessageBox.TYPE_INFO, timeout=10)
        else:
            self.session.open(MessageBox, _("Conversion failed: %s") % result[1], MessageBox.TYPE_ERROR, timeout=10)

    def conversion_finished(self, result):
        self["progress_source"].setValue(0)
        from twisted.internet import reactor
        reactor.callFromThread(self._show_conversion_result, result)

    def _check_conversion_status(self):
        """Check if the conversion was canceled successfully"""
        self.cancel_timer.stop()
        if self.is_converting:
            logger.warning("Conversion not cancelled properly, forcing termination")
            self._conversion_cancelled()

    def cancel_convert(self):
        """Cancel the ongoing conversion"""
        if self.is_converting:
            self.cancel_conversion = True
            self["key_blue"].setText(_("Cancelling..."))
            logger.info("Conversion cancellation requested")

            self.cancel_timer = eTimer()
            self.cancel_timer.callback.append(self._check_conversion_status)
            self.cancel_timer.start(1000)

    def _conversion_cancelled(self):
        """Handle conversion cancellation"""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))  # Ripristina il testo originale
        self.session.open(MessageBox, _("Conversion cancelled"), MessageBox.TYPE_INFO, timeout=5)

    def _conversion_error(self, error_msg):
        """Handle conversion error"""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))  # Ripristina il testo originale
        self.session.open(MessageBox, _("Conversion error: %s") % error_msg, MessageBox.TYPE_ERROR, timeout=10)

    def update_progress(self, value, text):
        from twisted.internet import reactor
        reactor.callFromThread(self._update_progress_ui, value, text)

    def _update_progress_ui(self, value, text):
        self.progress_source.setRange(len(self.m3u_list) if self.m3u_list else 100)
        self.progress_source.setValue(value)
        self["progress_text"].setText(text)

    def open_settings(self):
        self.session.open(M3UConverterSettings)

    def key_ok(self):
        index = self["list"].getSelectedIndex()
        if index < 0 or index >= len(self.m3u_list):
            self["status"].setText(_("No file selected"))
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
        try:
            if hasattr(self, 'initial_service') and self.initial_service:
                self.session.nav.stopService()

            stream = eServiceReference(4097, 0, url)
            stream.setName(name)
            logger.log("DEBUG", f"Extracted video URL: {str(stream)}")
            self.session.nav.playService(stream)

            if hasattr(self, 'aspect_manager'):
                self.aspect_manager.set_aspect_for_video()

        except Exception as e:
            logger.error(f"Error starting player: {str(e)}")
            self["status"].setText(_("Playback error"))

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
            if hasattr(self, 'initial_service') and self.initial_service:
                self.session.nav.playService(self.initial_service)

            if hasattr(self, 'aspect_manager'):
                self.aspect_manager.restore_aspect()

            self.close()
        except Exception as e:
            logger.error(f"Error during close: {str(e)}")
            self.close()

    def _show_conversion_result(self, result):
        """Show the conversion result (called in the main thread)"""
        try:
            if isinstance(result, tuple) and len(result) == 2:
                success, data = result
                if success:
                    if isinstance(data, tuple) and len(data) == 3:
                        total_channels, epg_channels, _ = data
                        msg = _("Successfully converted %d items") % total_channels
                        if epg_channels > 0:
                            msg += _("\nEPG mapped for %d channels") % epg_channels
                            msg += _("\nEPG files generated in /etc/epgimport/")

                        self.session.open(
                            MessageBox,
                            msg,
                            MessageBox.TYPE_INFO,
                            timeout=10
                        )
                    else:
                        self.session.open(
                            MessageBox,
                            _("Successfully converted items"),
                            MessageBox.TYPE_INFO,
                            timeout=5
                        )
                else:
                    self.session.open(
                        MessageBox,
                        _("Conversion failed: %s") % data,
                        MessageBox.TYPE_ERROR,
                        timeout=10
                    )
            else:
                self.session.open(
                    MessageBox,
                    _("Conversion completed with unknown result"),
                    MessageBox.TYPE_INFO,
                    timeout=5
                )
        except Exception as e:
            logger.error(f"Error showing conversion result: {str(e)}")

        self["status"].setText(_("Conversion completed"))
        self["progress_text"].setText("")

    def show_plugin_info(self):
        """Show plugin information and credits"""
        info = [
            f"Archimede Universal Converter v.{currversion}",
            _("Author: Lululla"),
            _("License: CC BY-NC-SA 4.0"),
            _("Developed for Enigma2"),
            _(f"Last modified: {last_date}"),
            "",
            _("------- Features -------"),
            _(" • Convert M3U playlists to bouquets"),
            _(" • M3U ➔ Enigma2 Bouquets"),
            _(" • Enigma2 Bouquets ➔ M3U"),
            _(" • JSON ➔ Enigma2 Bouquets"),
            _(" • XSPF ➔ M3U Playlist"),
            _(" • Remove M3U Bouquets"),
            _(" • Auto mapping IPTV/DVB-S/C/T"),
            _(" • Add EPG refs where available"),
            _(" • Simple and lightweight"),
            "",
            _("------- Usage -------"),
            _(" • Press Green to convert to TV"),
            _(" • Press OK to play a stream"),
            _(" • Press Back to return"),
            "",
            _("Enjoy your enhanced playlists!"),
            "",
            _("If you like this plugin, consider"),
            _("buying me a coffee ☕"),
            _("Scan the QR code to support development"),
            _("It helps keep the plugin alive"),
            _("and updated. Thank you!"),
            "",
            "bye bye Lululla",
            _("Press OK to close")
        ]
        self.session.open(
            MessageBox,
            "\n".join(info),
            MessageBox.TYPE_INFO,
            timeout=5
        )

    def show_info(self, message):
        logger.info(message)
        self.session.open(
            MessageBox,
            message,
            MessageBox.TYPE_INFO,
            timeout=5
        )
        self["status"].setText(message)

    def show_error(self, message):
        """Show error message and log it"""
        logger.error(message)
        self.session.open(
            MessageBox,
            message,
            MessageBox.TYPE_ERROR,
            timeout=5
        )
        self["status"].setText(message)


class M3UConverterSettings(Setup):
    def __init__(self, session, parent=None):
        Setup.__init__(self, session, setup="M3UConverterSettings", plugin="Extensions/M3UConverter")
        self.parent = parent

    def keySave(self):
        Setup.keySave(self)


def main(session, **kwargs):
    core_converter.cleanup_old_backups(config.plugins.m3uconverter.max_backups.value)
    session.open(ConversionSelector)


def get_pattern_for_type(conversion_type):
    patterns = {
        "m3u_to_tv": r"(?i)^.*\.(m3u|m3u8)$",
        "tv_to_m3u": r"(?i)^.*\.tv$",
        "json_to_tv": r"(?i)^.*\.json$",
        "xspf_to_m3u": r"(?i)^.*\.xspf$",
        "m3u_to_json": r"(?i)^.*\.(m3u|m3u8)$",
    }
    return patterns.get(conversion_type, r".*")


def Plugins(**kwargs):
    from Plugins.Plugin import PluginDescriptor
    return [PluginDescriptor(
        name=_("Universal Converter"),
        description=_("Convert between M3U and Enigma2 bouquets"),
        where=PluginDescriptor.WHERE_PLUGINMENU,
        icon="plugin.png",
        fnc=main)
    ]
