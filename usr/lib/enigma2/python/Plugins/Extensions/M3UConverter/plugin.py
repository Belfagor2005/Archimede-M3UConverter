# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 2.0                                         #
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

# ==================== IMPORTS ====================
# Standard library
import codecs
import json
import shutil
import unicodedata
from collections import defaultdict
from os import access, W_OK, listdir, remove, replace, chmod, system, mkdir, makedirs
from os.path import exists, isdir, isfile, join, normpath, basename, dirname, getsize
from re import sub, findall, DOTALL, MULTILINE, IGNORECASE, search
from threading import Lock
from time import strftime
from urllib.parse import unquote

# Third-party libraries
from twisted.internet import threads

# Enigma2 core
from enigma import eServiceReference, getDesktop, eTimer, eAVControl as AVSwitch

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

from . import _
from .Logger_clr import get_logger

# ==================== CONSTANTS AND GLOBALS ====================
currversion = '2.1'
last_date = "20250903"
title_plug = _("Archimede Universal Converter v.%s by Lululla") % currversion
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2

archimede_converter_path = "archimede_converter"
log_dir = join("/tmp", archimede_converter_path)
main_log = join(log_dir, "unified_converter.log")


# Create directory if it does not exist
try:
    makedirs(log_dir, exist_ok=True)
except Exception:
    pass


logger = get_logger(
    log_path=log_dir,
    plugin_name="M3U_CONVERTER",
    clear_on_start=True,
    max_size_mb=1
)


# ==================== UTILITY FUNCTIONS ====================
def defaultMoviePath():
    """Get default movie path from Enigma2 configuration"""
    result = config.usage.default_path.value
    if not result.endswith("/"):
        result += "/"
    if not isdir(result):
        return defaultRecordingLocation(config.usage.default_path.value)
    return result


def get_mounted_devices():
    """Get list of mounted and writable devices"""
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


def create_backup():
    """Create a backup of bouquets.tv only"""
    from shutil import copy2
    src = "/etc/enigma2/bouquets.tv"
    dst = "/etc/enigma2/bouquets.tv.bak"
    if exists(src):
        copy2(src, dst)


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
                time.sleep(2)
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
    """Convert accented characters to ASCII equivalents"""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode('ascii', 'ignore').decode('ascii')


def clean_group_name(name):
    """Clean group names preserving accented characters"""
    if not name:
        return "Default"

    cleaned = name.strip()

    # 1. Remove pipe prefixes: |IT|, |UK|, |FR|
    cleaned = sub(r'^\s*\|[A-Z]+\|\s*', '', cleaned)

    # 2. Remove colon prefixes: IT:, UK:, FR:
    cleaned = sub(r'^\s*[A-Z]{2}:\s*', '', cleaned)

    # 3. Remove text prefixes: IT , UK , FR
    cleaned = sub(r'^\s*(IT|UK|FR|DE|ES|NL|PL|GR|CZ|HU|RO|SE|NO|DK|FI)\s+', '', cleaned, flags=IGNORECASE)

    # 4. Remove emojis but preserve accented letters
    cleaned = sub(r'[^\w\s\-àèéìíòóùúÀÈÉÌÍÒÓÙÚ]', '', cleaned)

    # 5. Remove multiple spaces
    cleaned = ' '.join(cleaned.split())

    # 6. Shorten if too long
    if len(cleaned) > 40:
        cleaned = cleaned[:40]

    # 7. If empty after cleaning, use "Default"
    return cleaned or "Default"


# ==================== CONFIG INITIALIZATION ====================
config.plugins.m3uconverter = ConfigSubsection()
default_dir = config.movielist.last_videodir.value if isdir(config.movielist.last_videodir.value) else defaultMoviePath()
config.plugins.m3uconverter.lastdir = ConfigSelection(default=default_dir, choices=[])

config.plugins.m3uconverter.epg_enabled = ConfigYesNo(default=True)
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
config.plugins.m3uconverter.enable_debug = ConfigYesNo(default=False)
config.plugins.m3uconverter.language = ConfigSelection({
    "it": "Italiano",
    "en": "English",
    "de": "Deutsch",
    "fr": "Français",
    "es": "Español",
    "pt": "Português",
    "nl": "Nederlands",
    "tr": "Türkçe",
    "pl": "Polski",
    "gr": "Ελληνικά",
    "cz": "Čeština",
    "hu": "Magyar",
    "ro": "Română",
    "se": "Svenska",
    "no": "Norsk",
    "dk": "Dansk",
    "fi": "Suomi",
    "all": "All Cowntry - IPTV",
}, default="all")


update_mounts()


# ==================== CORE CLASSES ====================
class AspectManager:
    """Manage aspect ratio settings for video playback"""
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


# ==================== GLOBAL INSTANCES ====================

aspect_manager = AspectManager()
screenwidth = getDesktop(0).size()
screen_width = screenwidth.width()


class EPGServiceMapper:
    """Service mapper for EPG data matching and conversion"""
    def __init__(self, prefer_satellite=True):
        self.prefer_satellite = prefer_satellite
        self.channel_map = defaultdict(list)
        self.rytec_map = {}
        self.rytec_clean_map = {}
        self._clean_name_cache = {}
        self.rytec_extended_map = defaultdict(list)
        self._match_cache = {}
        self._match_cache_hits = 0
        self._match_cache_misses = 0
        self._cache_max_size = 5000
        self._incompatible_matches = 0
        self.enigma_config = self.load_enigma2_config()
        self.country_code = self.get_country_code()
        self.optimized_channel_map = {}
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("EPGServiceMapper initialized with prefer_satellite=%s, country_code=%s",
                        prefer_satellite, self.country_code)

    def clear_match_cache_only(self):
        """Clear only the match cache keeping loaded mappings"""
        cache_size = len(self._match_cache)
        self._match_cache.clear()
        self._match_cache_hits = 0
        self._match_cache_misses = 0
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Cleared match cache only ({cache_size} entries), keeping loaded mappings")

    def get_cache_stats(self):
        """Return detailed cache statistics"""
        total_requests = self._match_cache_hits + self._match_cache_misses

        # Calculate hit rate
        hit_rate = (self._match_cache_hits / total_requests * 100) if total_requests > 0 else 0

        # Analyze cache by type
        cache_analysis = {
            'compatible': 0,
            'incompatible': 0,
            'empty': 0
        }

        for result, match_type in self._match_cache.values():
            if not result:
                cache_analysis['empty'] += 1
            elif self.is_service_compatible(result):
                cache_analysis['compatible'] += 1
            else:
                cache_analysis['incompatible'] += 1

        return {
            'hits': self._match_cache_hits,
            'misses': self._match_cache_misses,
            'total_requests': total_requests,
            'hit_rate': f"{hit_rate:.1f}%",
            'cache_size': len(self._match_cache),
            'cache_analysis': cache_analysis,
            'incompatible_matches': self._incompatible_matches,
            'loaded_dvb_channels': len(self.channel_map),
            'rytec_channels': len(self.rytec_extended_map)
        }

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

    def load_channel_mapping(self, mapping_path="/usr/lib/enigma2/python/Plugins/Extensions/M3UConverter/channel_mapping.conf"):
        """Load channel ID mapping from external file with improved parsing"""
        self.channel_mapping = {}
        self.reverse_channel_mapping = {}  # Reverse map: channel ID -> satellite

        if not fileExists(mapping_path):
            logger.warning("Channel mapping file not found: %s", mapping_path)
            return False

        try:
            current_satellite = None

            with open(mapping_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()

                    if not line or line.startswith('#'):
                        continue

                    # Check for satellite section
                    if line.startswith('[') and line.endswith(']'):
                        current_satellite = line[1:-1].strip()
                        self.channel_mapping[current_satellite] = []
                        continue

                    # Add channel ID to current satellite
                    if current_satellite and line:
                        # Remove any comments after comma
                        channel_id = line.split(',')[0].strip()
                        self.channel_mapping[current_satellite].append(channel_id)

                        # Add to reverse map
                        self.reverse_channel_mapping[channel_id] = current_satellite
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Loaded channel mapping for %d satellites with %d total channels",
                            len(self.channel_mapping), len(self.reverse_channel_mapping))
            return True

        except Exception as e:
            logger.error("Error loading channel mapping: %s", str(e))
            return False

    def find_sky_channel_id(self, channel_name):
        """Find Sky channel ID based on name"""
        if not hasattr(self, 'reverse_channel_mapping'):
            return None

        # Cache to avoid repeated lookups
        cache_key = channel_name.lower()
        if hasattr(self, '_sky_cache') and cache_key in self._sky_cache:
            return self._sky_cache[cache_key]

        # Only if it is a Sky channel
        # if 'sky' not in channel_name.lower():
            # return None

        normalized_name = self.normalize_id(channel_name)
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"Searching Sky mapping for: '{channel_name}' -> normalized: '{normalized_name}'")

        # 1. Look for exact matches
        for channel_id in self.reverse_channel_mapping.keys():
            normalized_id = self.normalize_id(channel_id)
            if normalized_name == normalized_id:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Exact Sky match: {channel_id}")

                # Update cache
                if not hasattr(self, '_sky_cache'):
                    self._sky_cache = {}
                self._sky_cache[cache_key] = channel_id
                return channel_id

        # 2. Cerca corrispondenze parziali
        for channel_id in self.reverse_channel_mapping.keys():
            normalized_id = channel_id.lower().replace('.', '').replace('_', '').replace('-', '')

            # Se il nome normalizzato contiene l'ID o viceversa
            if normalized_name in normalized_id or normalized_id in normalized_name:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Partial name match: {channel_id} (similarity)")
                return channel_id

        # 2. Look for partial matches only for Sky
        sky_keywords = [
            'skycinema', 'skysport', 'skyatlantic', 'skycomedy',
            'skycrime', 'skynature', 'skynews', 'skyuno', 'skywitness',
            'skyshowcase', 'skyarts', 'skyanimation', 'skymax', 'skymix',
            'skyreplay'
        ]

        for keyword in sky_keywords:
            if keyword in normalized_name:
                for channel_id in self.reverse_channel_mapping.keys():
                    normalized_id = self.normalize_id(channel_id)
                    if keyword in normalized_id:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Sky keyword match: {channel_id} (via {keyword})")

                        # Update cache
                        if not hasattr(self, '_sky_cache'):
                            self._sky_cache = {}
                        self._sky_cache[cache_key] = channel_id
                        return channel_id

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"No Sky mapping found for: {channel_name}")
        return None

    def is_service_compatible(self, service_ref):
        """Check if service is compatible with current configuration - LESS RESTRICTIVE"""
        if not service_ref:
            return True

        # If it's an IPTV service, it's always compatible
        if service_ref.startswith('4097:'):
            return True

        # For DVB services, use more permissive logic
        parts = service_ref.split(':')
        if len(parts) < 11:
            return False

        try:
            service_type = parts[2]

            # Terrestrial and cable services are always compatible
            if service_type in ['16', '10']:  # DVB-T and DVB-C
                return True

            # For satellite services, verify only main satellites
            onid = int(parts[5], 16) if parts[5] else 0

            # Reduced list of compatible ONIDs (only main satellites)
            compatible_onids = {
                0x13E, 0x110,   # 13.0°E Hotbird
                0x1, 0x2,       # 19.2°E Astra, 28.2°E Astra
                0x11, 0x10,     # 23.5°E Astra, 9.0°E Eutelsat
                0xFFFF, 0xEEEE  # Special services (cable/terrestrial)
            }
            return onid in compatible_onids

        except (ValueError, IndexError):
            return False

    def normalize_id(self, id_string):
        """Normalize an ID for matching by removing special characters"""
        if not id_string:
            return ""

        normalized = id_string.lower()
        # Remove common special characters
        normalized = normalized.replace('.', '').replace('_', '').replace('-', '').replace(' ', '')
        # Remove quality indicators
        normalized = normalized.replace('hd', '').replace('hevc', '').replace('h265', '').replace('h264', '')
        normalized = normalized.replace('4k', '').replace('uhd', '').replace('fhd', '')

        return normalized

    def _original_is_service_compatible(self, service_ref):
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
                0xFFFF:  0,     # Cable services (special ONID)
                0xEEEE:  0,     # Terrestrial services (special ONID)
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
        """Get country code from plugin configuration"""
        try:
            if hasattr(config.plugins, 'm3uconverter') and hasattr(config.plugins.m3uconverter, 'language'):
                return config.plugins.m3uconverter.language.value

            country_code = "it"  # Default to Italy
            settings_path = "/etc/enigma2/settings"
            if not fileExists(settings_path):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("Enigma2 settings file not found, using default country code: %s", country_code)
                return country_code

            with open(settings_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('config.misc.country='):
                        country_code = line.split('=', 1)[1].strip()
                        break
            return country_code.lower()
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("Error reading country code: {0}".format(str(e)))
            return "it"

    def clean_channel_name(self, name):
        """Clean channel name for matching"""
        if not name:
            return ""

        if name in self._clean_name_cache:
            return self._clean_name_cache[name]

        try:
            cleaned = name.lower().strip()

            # Remove prefixes like |IT|, |UK|, |US|, etc. at the beginning
            cleaned = sub(r'^\s*\|[A-Z]{2,}\|\s*', '', cleaned)

            # Also remove other common prefixes with pipes
            cleaned = sub(r'^\s*\|[^|]+\|\s*', '', cleaned)

            # Remove numbers in parentheses, e.g. "(6)" or "[5]"
            cleaned = sub(r'\s*[\(\[]\d+[\)\]]\s*', ' ', cleaned)

            # Remove quality indicators
            quality_patterns = [
                r'\b(4k|uhd|fhd|hd|sd|hq|uhq|sdq|hevc|h265|h264|h\.265|h\.264)\b',
                r'\b(full hd|ultra hd|high definition|standard definition)\b',
                r'\s*\(\d+p\)', r'\s*\d+p'
            ]

            for pattern in quality_patterns:
                cleaned = sub(pattern, '', cleaned, flags=IGNORECASE)

            # Remove special characters
            for char in '()[]{}|/\\_—–-+':
                cleaned = cleaned.replace(char, ' ')

            cleaned = sub(r'[^a-z0-9\s]', '', cleaned)
            cleaned = ' '.join(cleaned.split()).strip()

            if not cleaned:
                cleaned = sub(r'[^a-z0-9]', '', name.lower())
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("Channel name '{0}' resulted in empty string, using fallback: '{1}'".format(name, cleaned))

            self._clean_name_cache[name] = cleaned
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Cleaned channel name: '{name}' -> '{cleaned}'")
            return cleaned

        except Exception as e:
            logger.error(f"Error cleaning channel name '{name}': {str(e)}")
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
        if config.plugins.m3uconverter.enable_debug.value:
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
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Parsed {0} unique compatible DVB channel names from {1}".format(len(self.channel_map), lamedb_path))
                return True

            except Exception as e:
                logger.error("Error parsing {0}: {1}".format(lamedb_path, str(e)))

        if config.plugins.m3uconverter.enable_debug.value:
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

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Rytec file found, size: %d bytes", len(content))
            # CORRECT PATTERN for comments BEFORE and AFTER
            pattern = r'(<!--\s*([^>]+)\s*-->)?\s*<channel id="([^"]+)">([^<]+)</channel>\s*(?:<!--\s*([^>]+)\s*-->)?'
            matches = findall(pattern, content)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Found %d channel entries in rytec file", len(matches))

            for match in matches:
                comment_before, source_info, channel_id, service_ref, comment_after = match

                # PICK the RIGHT COMMENT (before or after)
                comment = comment_before or comment_after or ""

                # EXTRACT the REAL CHANNEL NAME
                channel_name = self._extract_real_channel_name(comment)

                normalized_ref = self.normalize_service_reference(service_ref, for_epg=True)

                if self.is_service_compatible(normalized_ref):
                    # EXTENDED DATABASE with ALL INFO
                    self.rytec_extended_map[channel_id].append({
                        'sref': normalized_ref,
                        'comment': comment.strip(),
                        'channel_name': channel_name,
                        'source_type': self._get_source_type(comment),
                        'sat_position': self._extract_sat_position(comment)
                    })

                    # KEEP COMPATIBILITY
                    if channel_id not in self.rytec_map:
                        self.rytec_map[channel_id] = normalized_ref

                    clean_base_id = self.clean_channel_name(channel_id.split('.')[0])
                    self.rytec_clean_map[clean_base_id] = normalized_ref

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Parsed %d Rytec channels with extended info", len(self.rytec_extended_map))

            # # Log alcune statistiche
            # sky_count = len([id for id in self.rytec_map.keys() if 'sky' in id.lower()])
            # italian_count = len([id for id in self.rytec_map.keys() if '.it' in id.lower()])

            # logger.info("Rytec stats: %d Sky channels, %d Italian channels", sky_count, italian_count)

        except Exception as e:
            logger.error("Error parsing rytec.channels.xml: %s", str(e))

    def _extract_real_channel_name(self, comment):
        """Extract the real channel name from the FINAL comment"""
        if not comment:
            return ""

        # LOOK for the LAST part after the last -->
        parts = comment.split('-->')
        if len(parts) > 1:
            return parts[-1].strip()

        return comment.strip()

    def _extract_sat_position(self, comment):
        """Extract the satellite position from the comment"""
        position_match = search(r'(\d+\.\d+[EW])', comment)
        return position_match.group(1) if position_match else None

    def _get_source_type(self, comment):
        """Determine the source type - IMPROVED VERSION"""
        if not comment:
            return 'unknown'

        comment_lower = comment.lower()

        if any(x in comment_lower for x in [
            '13.0e', '19.2e', '5.0w', '9.0e', '8.0w', '7.0w', '4.8e', '4.0w',
            '45.0e', '42.0e', '39.0e', '36e', '33.0e', '31.5e', '30.0w', '28.4e',
            '26.0e', '23.5e', '16.0e', '15w', '15.0w', '1.9e', '0.8w'
        ]):
            return 'satellite'

        if 'iptv' in comment_lower or 'http' in comment_lower:
            return 'iptv'

        if 'misc' in comment_lower:
            return 'misc'

        if 'terrestre' in comment_lower or 'dvb-t' in comment_lower:
            return 'terrestrial'

        if 'cable' in comment_lower:
            return 'cable'

        return 'unknown'

    def filter_compatible_services(self, services):
        """Keep only compatible service references"""
        compatible_services = []
        for service in services:
            if self.is_service_compatible(service['sref']):
                compatible_services.append(service)
        return compatible_services

    def find_best_service_match(self, clean_name, tvg_id=None, original_name=""):
        """Enhanced matching with Sky-specific handling and full integration"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"SEARCHING FOR: '{original_name}' -> clean: '{clean_name}', tvg_id: '{tvg_id}'")

        # Log del mapping caricato
        if hasattr(self, 'reverse_channel_mapping'):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Loaded {len(self.reverse_channel_mapping)} channel IDs in mapping")

        # Normalizza per la cache
        normalized_clean = self.normalize_id(clean_name)
        normalized_tvg = self.normalize_id(tvg_id) if tvg_id else ''
        normalized_original = self.normalize_id(original_name) if original_name else ''
        cache_key = f"{normalized_clean}|{normalized_tvg}|{normalized_original}"

        # -------------------------
        # Sky-specific search
        # -------------------------
        # Exact tvg_id
        if tvg_id and tvg_id in self.rytec_map:
            sref = self.rytec_map[tvg_id]
            if self.is_service_compatible(sref):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Exact tvg_id match in rytec: {tvg_id} -> {sref}")
                return sref, 'rytec_exact'
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"tvg_id found but not compatible: {tvg_id}")

        # Varianti tvg_id
        if tvg_id:
            tvg_variants = [
                tvg_id,
                tvg_id.lower(),
                tvg_id.upper(),
                tvg_id.replace('.', ''),
                tvg_id.replace('.', '').lower(),
                tvg_id.replace(' ', ''),
                tvg_id.replace(' ', '').lower(),
            ]
            for variant in tvg_variants:
                if variant in self.rytec_map:
                    sref = self.rytec_map[variant]
                    if self.is_service_compatible(sref):
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"tvg_id variant match: {variant} -> {sref}")
                        return sref, 'rytec_variant'

        # Mapping Sky
        # Check if it's a Sky channel before doing heavy research
        is_sky_channel = 'sky' in original_name.lower() or (tvg_id and 'sky' in tvg_id.lower())

        if is_sky_channel:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Sky channel detected: {original_name}")

            # 3a. Cerca nel mapping dei canali
            mapped_id = self.find_sky_channel_id(original_name)
            if mapped_id:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f" Found in channel mapping: {mapped_id}")

                # 3b. Cerca l'ID mappato in rytec
                if mapped_id in self.rytec_map:
                    sref = self.rytec_map[mapped_id]
                    if self.is_service_compatible(sref):
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Mapped ID in rytec: {mapped_id} -> {sref}")
                        return sref, 'sky_mapping_rytec'

                # 3c. Cerca varianti dell'ID mappato in rytec
                mapped_variants = [
                    mapped_id.lower(),
                    mapped_id.upper(),
                    mapped_id.replace('.', ''),
                    mapped_id.replace('.', '').lower(),
                ]

                for variant in mapped_variants:
                    if variant in self.rytec_map:
                        sref = self.rytec_map[variant]
                        if self.is_service_compatible(sref):
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.debug(f"Mapped variant in rytec: {variant} -> {sref}")
                            return sref, 'sky_mapping_variant'

        # 3d. DEBUG: Log Sky IDs only for Sky channels
        if config.plugins.m3uconverter.enable_debug.value:
            # Get all IDs in rytec that contain "sky"
            sky_ids_in_rytec = [id for id in self.rytec_map.keys() if 'sky' in id.lower()]

            if sky_ids_in_rytec:
                logger.debug(f"Sky IDs available in rytec: {len(sky_ids_in_rytec)}")
                # Log only the first 5 to avoid flooding the logs
                for sky_id in sky_ids_in_rytec[:5]:
                    logger.debug(f"   - {sky_id} -> {self.rytec_map[sky_id]}")
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("NO Sky IDs found in rytec map!")

        # -------------------------
        # Check cache
        # -------------------------
        if cache_key in self._match_cache:
            cached_result, cached_type = self._match_cache[cache_key]
            self._match_cache_hits += 1
            if cached_result and self.is_service_compatible(cached_result):
                return cached_result, f"cached_{cached_type}"
            else:
                self._match_cache_misses += 1
        else:
            self._match_cache_misses += 1

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"MATCHING: '{original_name}' -> clean: '{clean_name}', tvg_id: '{tvg_id}'")

        # -------------------------
        # Check if the rytec file was loaded correctly
        if not hasattr(self, 'rytec_extended_map') or not self.rytec_extended_map:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("rytec_extended_map NOT LOADED or EMPTY")

        # 3️⃣ Universal system
        # -------------------------
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("Trying UNIVERSAL matching...")
        universal_match, match_type = self.find_universal_service_match(clean_name, tvg_id, original_name)
        if universal_match:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"UNIVERSAL MATCH: {match_type} -> {universal_match}")
            # Memorizza in cache SOLO se compatibile
            if self.is_service_compatible(universal_match):
                self._add_to_cache(cache_key, universal_match, f'universal_{match_type}')
            return universal_match, f'universal_{match_type}'

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("NO UNIVERSAL MATCH - Checking why...")

        # -------------------------
        # 4️⃣ Name variations
        # -------------------------
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("Trying NAME VARIATIONS...")

        name_variations = self.generate_name_variations(clean_name)
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"Generated variations: {name_variations}")

        for variation in name_variations:
            if variation in self.rytec_clean_map:
                sref = self.rytec_clean_map[variation]
                if self.is_service_compatible(sref):
                    self._add_to_cache(cache_key, sref, 'rytec_variation')
                    return sref, 'rytec_variation'
                else:
                    self._incompatible_matches += 1

            if variation in self.optimized_channel_map:
                sref = self.optimized_channel_map[variation]['sref']
                if self.is_service_compatible(sref):
                    self._add_to_cache(cache_key, sref, 'lamedb_variation')
                    return sref, 'lamedb_variation'
                else:
                    self._incompatible_matches += 1

        # -------------------------
        # 5️⃣ Extended database fallback
        # -------------------------
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("Searching in EXTENDED database by name...")
        best_extended = None
        best_score = 0

        for channel_id, variants in self.rytec_extended_map.items():
            for variant in variants:
                if variant['channel_name']:
                    score = self._calculate_similarity(clean_name, variant['channel_name'])
                    if score > 0.6 and score > best_score:
                        best_score = score
                        best_extended = variant['sref']
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Extended match: {variant['channel_name']} -> score: {score}")

        if best_extended and best_score > 0.7:
            compatible = self.is_service_compatible(best_extended)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"EXTENDED MATCH: score {best_score} -> {best_extended} (compatible: {compatible})")
            if compatible:
                self._add_to_cache(cache_key, best_extended, 'extended_fallback')
                return best_extended, 'extended_fallback'
            else:
                self._incompatible_matches += 1

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("NO MATCH FOUND AT ALL")
        return None, None

    def _add_to_cache(self, cache_key, result, match_type):
        """Add to cache only if compatible and manage space"""
        if result and self.is_service_compatible(result):
            if len(self._match_cache) >= self._cache_max_size:
                # Remove the oldest 10% when cache is full
                items_to_remove = int(self._cache_max_size * 0.1)
                for key in list(self._match_cache.keys())[:items_to_remove]:
                    del self._match_cache[key]

            self._match_cache[cache_key] = (result, match_type)

    def generate_name_variations(self, name):
        """Generate variations of a channel name for matching"""
        variations = set()
        variations.add(name)
        variations.add(name.replace(' ', ''))
        variations.add(name.replace(' ', '_'))
        variations.add(sub(r'\d+', '', name))
        return variations

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
                # For EPG, we need to extract the DVB part from IPTV reference
                # This is complex - always use DVB references for the EPG
                parts = dvb_sref.split(':')
                if len(parts) >= 11:
                    # Try converting IPTV back to DVB (not always possible)
                    service_type = parts[2]
                    service_id = parts[3]
                    ts_id = parts[4]
                    on_id = parts[5]
                    namespace = parts[6]
                    return f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"
            return dvb_sref
            """
                # if len(parts) > 10:
                    # return ':'.join(parts[:10]) + ':'
            # return dvb_sref
            """
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
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("Generated service reference: %s", sref)
        return sref

    def generate_epg_channels_file(self, epg_data, bouquet_name):
        """Generate channels.xml file compatible with EPGImport - USE IPTV REFERENCES"""
        epgimport_path = "/etc/epgimport"
        epg_filename = "%s.channels.xml" % bouquet_name
        epg_path = join(epgimport_path, epg_filename)

        if not fileExists(epgimport_path):
            try:
                mkdir(epgimport_path)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Created epgimport directory: %s", epgimport_path)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Could not create epgimport directory: %s", str(e))
                return False

        try:
            with open(epg_path, 'w', encoding="utf-8") as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<channels>\n')

                for channel in epg_data:
                    if channel.get('sref'):
                        channel_name = channel.get('name', 'Unknown')
                        channel_id = self._normalize_channel_id_for_epgimport(channel_name)

                        epg_sref = self.normalize_service_reference(channel['sref'], True)
                        f.write('  <channel id="%s">%s</channel>\n' % (channel_id, epg_sref))
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug("EPG Channel: %s -> %s", channel_id, epg_sref)
                    else:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.warning("Skipping channel due to missing sref: %s", channel.get('name'))

                f.write('</channels>\n')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Generated EPG channels file with channel names: %s", epg_path)
            return True

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("Error generating EPG channels file: %s", str(e))
            return False

    def _normalize_channel_id_for_epgimport(self, channel_name):
        """Normalize channel name for matching with EPGImport"""
        if not channel_name:
            return "unknown"

        # Convert to lowercase and remove special characters
        cleaned = channel_name.lower()

        # Remove quality indicators but keep the base name
        quality_indicators = [
            'h265', 'h264', 'hevc', '4k', 'uhd', 'fhd', 'hd', 'sd',
            'hq', 'uhq', 'sdq', 'stream', 'live', 'tv'
        ]

        for indicator in quality_indicators:
            cleaned = sub(r'\b%s\b' % indicator, '', cleaned)

        # Remove symbols and multiple spaces
        cleaned = sub(r'[^a-z0-9\s]', ' ', cleaned)
        cleaned = ' '.join(cleaned.split()).strip()
        return cleaned
        """
        # # Fallback mapping for common names
        # name_mapping = {
            # 'canale5': 'canale 5',
            # 'italia1': 'italia 1',
            # 'italia2': 'italia 2',
            # 'la7': 'la7',
            # 'la7d': 'la7d',
            # 'rai1': 'rai 1',
            # 'rai2': 'rai 2',
            # 'rai3': 'rai 3',
            # 'rai4': 'rai 4',
            # 'rai5': 'rai 5',
            # 'rete4': 'rete 4',
            # 'sky arte': 'sky arte',
            # 'sky atlantic': 'sky atlantic',
            # 'sky cinema uno': 'sky cinema uno',
            # 'sky crime': 'sky crime',
            # 'sky documentari': 'sky documentari',
            # 'sky explorer': 'sky explorer',
            # 'sky investigation': 'sky investigation',
            # 'sky nature': 'sky nature',
            # 'sky series': 'sky series',
            # 'sky sport uno': 'sky sport uno',
            # 'sky tg24': 'sky tg24',
            # 'sky uno': 'sky uno',
            # 'skyarte': 'sky arte',
            # 'skyatlantic': 'sky atlantic',
            # 'skycinema': 'sky cinema',
            # 'skynature': 'sky nature',
            # 'skysport': 'sky sport',
            # 'skytg24': 'sky tg24',
            # 'skyuno': 'sky uno',
            # 'telecolor': 'telecolor'
        # }

        # # Apply mapping if exists, otherwise use the cleaned value
        # return name_mapping.get(cleaned, cleaned)
        """

    def _clean_epg_channel_id(self, channel_id):
        """Clean the channel ID for EPGImport"""
        if not channel_id:
            return ""

        # Remove problematic characters for XML
        cleaned = channel_id.replace('&', 'and').replace('<', '').replace('>', '').replace('"', '')
        cleaned = cleaned.replace('+', 'plus').replace('#', '').replace('*', '')

        return cleaned.strip()

    def generate_epg_sources_file(self, bouquet_name, epg_url=None):
        """Generate sources.xml that correctly points to the channels files"""
        epgimport_path = "/etc/epgimport"
        sources_filename = "ArchimedeConverter.sources.xml"
        sources_path = join(epgimport_path, sources_filename)

        try:
            # Create directory if missing
            if not fileExists(epgimport_path):
                mkdir(epgimport_path)

            # Read or initialize the file
            if fileExists(sources_path):
                with open(sources_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n</sources>'

            # Remove old source for this bouquet
            pattern = r'<source type="gen_xmltv"[^>]*channels="%s\.channels\.xml"[^>]*>.*?</source>' % bouquet_name
            content = sub(pattern, '', content, flags=DOTALL)

            # CREATE the NEW correct SOURCE
            new_source = '    <source type="gen_xmltv" nocheck="1" channels="%s.channels.xml">\n' % bouquet_name
            new_source += '      <description>%s</description>\n' % bouquet_name

            if epg_url:
                new_source += '      <url><![CDATA[%s]]></url>\n' % epg_url
            else:
                # Add default URLs based on language
                language_code = self.get_country_code().upper()
                urls = self._get_epg_urls_for_language(language_code)
                for url in urls:
                    new_source += '      <url>%s</url>\n' % url

            new_source += '    </source>\n'

            # Add to content
            sourcecat_marker = '<sourcecat sourcecatname="Archimede Converter by Lululla">'
            if sourcecat_marker in content:
                content = content.replace(sourcecat_marker, sourcecat_marker + '\n' + new_source)
            else:
                new_sourcecat = '  <sourcecat sourcecatname="Archimede Converter by Lululla">\n'
                new_sourcecat += new_source
                new_sourcecat += '  </sourcecat>\n'
                content = content.replace('</sources>', new_sourcecat + '</sources>')

            # Write file
            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(content)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Updated EPG sources file: %s", sources_path)
            return True

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("Error generating EPG sources file: %s", str(e))
            return False

    def generate_epg_sources_file2(self, bouquet_name, epg_url=None):
        """Generate EPG sources file for epgimport in the proper format"""
        epgimport_path = "/etc/epgimport"
        sources_filename = "ArchimedeConverter.sources.xml"
        sources_path = join(epgimport_path, sources_filename)

        # Create directory if missing
        if not fileExists(epgimport_path):
            try:
                mkdir(epgimport_path)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Created epgimport directory: %s", epgimport_path)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Could not create epgimport directory: %s", str(e))
                return False

        # Get language from configuration
        try:
            if hasattr(config.plugins, 'm3uconverter') and hasattr(config.plugins.m3uconverter, 'language'):
                language_code = config.plugins.m3uconverter.language.value.upper()
            else:
                language_code = "ALL"
        except:
            language_code = "ALL"

        # Comprehensive language to source mapping based on your XML
        language_to_sources = {
            'ALL': [
                ('All Countries', 'https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz')
            ],
            'IT': [
                ('Italy', 'https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz'),
                ('DTT-SAT Italia', 'http://epg-guide.com/dttsat.xz'),
                ('Italia - Sky (xz)', 'http://www.xmltvepg.nl/rytecIT_Sky.xz')
            ],
            'EN': [
                ('UK', 'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz'),
                ('USA', 'https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz'),
                ('Australia', 'https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz'),
                ('Canada', 'https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz')
            ],
            'DE': [
                ('Germany', 'https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz'),
                ('Switzerland', 'http://www.xmltvepg.nl/rytecCH_Basic.xz'),
                ('Rakuten Germany', 'https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN_DE1.xml.gz')
            ],
            'FR': [
                ('France', 'https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz'),
                ('Rakuten France', 'https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN_FR1.xml.gz')
            ],
            'ES': [
                ('Spain', 'https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz'),
                ('Rakuten Spain', 'https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN_ES1.xml.gz')
            ],
            'NL': [
                ('Netherlands', 'https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz'),
                ('Rakuten Netherlands', 'https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN_NL1.xml.gz')
            ],
            'PL': [
                ('Poland', 'https://epgshare01.online/epgshare01/epg_ripper_PL1.xml.gz'),
                ('Rakuten Poland', 'https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN_PL1.xml.gz')
            ],
            # Other countries with ONLY the URLs you actually have
            'AR': [('Argentina', 'https://epgshare01.online/epgshare01/epg_ripper_AR1.xml.gz')],
            'BR': [('Brazil', 'https://epgshare01.online/epgshare01/epg_ripper_BR1.xml.gz')],
            'TR': [('Turkey', 'https://epgshare01.online/epgshare01/epg_ripper_TR1.xml.gz')],
            'GR': [('Greece', 'https://epgshare01.online/epgshare01/epg_ripper_GR1.xml.gz')],
            'CZ': [('Czech', 'https://epgshare01.online/epgshare01/epg_ripper_CZ1.xml.gz')],
            'HU': [('Hungary', 'https://epgshare01.online/epgshare01/epg_ripper_HU1.xml.gz')],
            'RO': [
                ('Romania 1', 'https://epgshare01.online/epgshare01/epg_ripper_RO1.xml.gz'),
                ('Romania 2', 'https://epgshare01.online/epgshare01/epg_ripper_RO2.xml.gz')
            ],
            'SE': [('Sweden', 'https://epgshare01.online/epgshare01/epg_ripper_SE1.xml.gz')],
            'NO': [('Norway', 'https://epgshare01.online/epgshare01/epg_ripper_NO1.xml.gz')],
            'DK': [('Denmark', 'https://epgshare01.online/epgshare01/epg_ripper_DK1.xml.gz')],
            'FI': [('Finland', 'https://epgshare01.online/epgshare01/epg_ripper_FI1.xml.gz')],
        }

        selected_sources = language_to_sources.get(language_code, language_to_sources['ALL'])

        try:
            # Read existing file or initialize new
            if fileExists(sources_path):
                with open(sources_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n</sources>'

            # Add new source
            new_source = '    <source type="gen_xmltv" nocheck="1" channels="%s.channels.xml">\n' % bouquet_name
            new_source += '      <description>%s (%s)</description>\n' % (bouquet_name, language_code)

            for desc, url in selected_sources:
                new_source += '      <url>%s</url>\n' % url

            new_source += '    </source>\n'

            # Check if the sourcecat already exists
            sourcecat_pattern = r'<sourcecat sourcecatname="Archimede Converter by Lululla">(.*?)</sourcecat>'
            sourcecat_match = search(sourcecat_pattern, content, DOTALL)

            if sourcecat_match:
                # Check if the sourcecat already exists
                existing_source_pattern = r'<source type="gen_xmltv"[^>]*channels="%s\.channels\.xml"[^>]*>.*?</source>' % bouquet_name
                if search(existing_source_pattern, content, DOTALL):
                    # Sostituisci la source esistente
                    content = sub(existing_source_pattern, new_source, content, flags=DOTALL)
                else:
                    # Add the new source
                    existing_content = sourcecat_match.group(1)
                    updated_content = existing_content + '\n' + new_source
                    content = content.replace(sourcecat_match.group(0),
                                              '<sourcecat sourcecatname="Archimede Converter by Lululla">%s</sourcecat>' % updated_content)
            else:
                # Create new sourcecat
                new_sourcecat = '  <sourcecat sourcecatname="Archimede Converter by Lululla">\n'
                new_sourcecat += new_source
                new_sourcecat += '  </sourcecat>\n'
                content = content.replace('</sources>', new_sourcecat + '</sources>')

            # Write back
            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Updated EPG sources for bouquet %s", bouquet_name)
            return True

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("Error updating EPG sources: %s", str(e))
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
        """Initialize with channel mapping and cache"""
        try:
            self.parse_lamedb()
            self.parse_rytec_channels()
            self.load_channel_mapping()
            self.optimize_matching()

            # Initialize cache
            self._sky_cache = {}
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Channel mapping loaded: %d satellites, %d channels",
                            len(self.channel_mapping), len(self.reverse_channel_mapping))
            return True

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("Failed to initialize EPGServiceMapper: %s", str(e))
            return False

    def get_channel_ids_for_satellite(self, satellite_position):
        """Get all channel IDs for a specific satellite"""
        return self.channel_mapping.get(satellite_position, [])

    def get_satellite_for_channel_id(self, channel_id):
        """Find which satellite a channel ID belongs to"""
        for satellite, channel_ids in self.channel_mapping.items():
            if channel_id in channel_ids:
                return satellite
        return None

    def find_universal_service_match(self, clean_name, tvg_id, original_name):
        """Search for a match in the extended Rytec database"""
        if not hasattr(self, 'rytec_extended_map'):
            return None, None

        candidates = []

        # 1. Search by exact ID in the extended database
        if tvg_id and tvg_id in self.rytec_extended_map:
            for variant in self.rytec_extended_map[tvg_id]:
                if self.is_service_compatible(variant['sref']):
                    candidates.append({
                        'sref': variant['sref'],
                        'score': 100,  # Exact match
                        'type': 'exact_id'
                    })

        # 2. Search by name similarity in the comments
        for channel_id, variants in self.rytec_extended_map.items():
            for variant in variants:
                if variant['channel_name']:
                    similarity = self._calculate_similarity(clean_name, variant['channel_name'])
                    if similarity > 0.7 and self.is_service_compatible(variant['sref']):
                        candidates.append({
                            'sref': variant['sref'],
                            'score': int(similarity * 100),
                            'type': 'name_similarity'
                        })

        # 3. Select the best candidate
        if candidates:
            candidates.sort(key=lambda x: x['score'], reverse=True)
            best = candidates[0]
            return best['sref'], f'universal_{best["type"]}'

        return None, None

    def _calculate_similarity(self, name1, name2):
        """Compute similarity between two names"""
        if not name1 or not name2:
            return 0

        if name1 in name2 or name2 in name1:
            return 0.7

        if name1 == name2:
            return 1.0

        try:
            from difflib import SequenceMatcher
            return SequenceMatcher(None, name1, name2).ratio()
        except ImportError:
            common_chars = set(name1) & set(name2)
            return len(common_chars) / max(len(set(name1)), len(set(name2)))

    def debug_rytec_content(self, search_term="sky"):
        """Debug per vedere cosa c'è nel file rytec"""
        if not hasattr(self, 'rytec_map'):
            return "Rytec map not loaded"

        matches = []
        for channel_id, sref in self.rytec_map.items():
            if search_term.lower() in channel_id.lower():
                matches.append(f"{channel_id} -> {sref}")

        return matches if matches else f"No matches for '{search_term}' in rytec"


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


# ==================== SCREEN CLASSES ====================


class M3UFileBrowser(Screen):
    def __init__(self, session, startdir="/etc/enigma2", matchingPattern=r"(?i)^.*\.(tv|m3u|m3u8|json|xspf)$", conversion_type=None, title=None):
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
                continue

            file_data = entry[0]
            path = None
            is_dir = False

            if len(file_data) >= 2:
                path = file_data[0]
                is_dir = file_data[1]

            elif len(file_data) == 1 and isinstance(file_data[0], str):
                path = file_data[0]
                is_dir = True
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Skipping invalid entry: {file_data}")
                continue

            if path == ".." or is_dir:
                filtered.append(entry)
            else:
                if path and path.lower().endswith(".tv") and self._contains_http(path):
                    filtered.append(entry)

        self["filelist"].list = filtered
        self["filelist"].l.setList(filtered)

    def _contains_http(self, filename):
        """Check if file contains 'http' (case-insensitive) with full path"""
        try:
            current_dir = self["filelist"].getCurrentDirectory()
            full_path = join(current_dir, filename)

            with open(full_path, "r") as f:
                return any("http" in line.lower() for line in f)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error reading {full_path}: {str(e)}")
            return False

    def ok_pressed(self):
        selection = self["filelist"].getCurrent()
        if not selection or not isinstance(selection, list) or not isinstance(selection[0], tuple):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Invalid selection format: {selection}")
            return

        file_data = selection[0]
        path = file_data[0]
        is_dir = file_data[1]
        dir_icon = None
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"file_data: {file_data}, path: {path}, is_dir: {is_dir}")
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
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Selected full file path: {full_path}")
                self.close(full_path)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"ok_pressed error: {str(e)}")

    def close(self, result=None):
        try:
            super(M3UFileBrowser, self).close(result)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error closing browser: {str(e)}")
            super(M3UFileBrowser, self).close(None)


class ConversionSelector(Screen):
    """Main conversion selector screen"""
    skin = """
        <screen name="ConversionSelector" position="center,center" size="1280,720" title="..::ConversionSelector::.." backgroundColor="#20000000" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="23,608" size="1185,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1220,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <widget name="info" position="0,0" size="1,1" font="Regular;1" transparent="1" />
            <widget name="text" position="0,0" size="1,1" font="Regular;1" transparent="1" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="25,700" size="250,6" zPosition="12" />
            <widget name="key_red" position="25,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="25,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="280,700" size="250,6" zPosition="12" />
            <widget name="key_green" position="280,660" size="250,45" zPosition="11" font="Regular; 28" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="280,660" size="250,45" zPosition="11" font="Regular; 28" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="541,700" size="250,6" zPosition="12" />
            <widget name="key_yellow" position="539,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="539,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="798,700" size="250,6" zPosition="12" />
            <widget name="key_blue" position="797,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="797,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    def __init__(self, session):
        Screen.__init__(self, session)
        self.session = session
        self.skinName = "ConversionSelector"
        self.is_modal = True
        self.setTitle(title_plug)
        self.menu = [
            (_("M3U ➔ Enigma2 Bouquets"), "m3u_to_tv", "m3u"),
            (_("Enigma2 Bouquets ➔ M3U"), "tv_to_m3u", "tv"),
            (_("JSON ➔ Enigma2 Bouquets"), "json_to_tv", "json"),
            (_("JSON ➔ M3U"), "json_to_m3u", "json"),
            (_("XSPF ➔ M3U Playlist"), "xspf_to_m3u", "xspf"),
            (_("M3U ➔ JSON"), "m3u_to_json", "m3u"),
            (_("Remove M3U Bouquets"), "purge_m3u_bouquets", None)
        ]
        self["list"] = MenuList([(x[0], x[1]) for x in self.menu])
        self["Title"] = Label(title_plug)
        self["info"] = Label('')
        self["text"] = Label('')
        self["status"] = Label(_("We're ready: what do you want to do?"))
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MenuActions"], {
            "red": self.close,
            "green": self.select_item,
            "blue": self.open_epgimporter,
            "menu": self.open_settings,
            "ok": self.select_item,
            "yellow": self.purge_m3u_bouquets,
            "cancel": self.close
        })
        self["key_red"] = StaticText(_("Close"))
        self["key_green"] = StaticText(_("Select"))
        self["key_yellow"] = StaticText(_("Remove Bouquets"))
        self["key_blue"] = StaticText(_("EPGImporter"))

    def open_settings(self):
        self.session.open(M3UConverterSettings)

    def open_epgimporter(self):
        from Plugins.Extensions.EPGImport.plugin import EPGImportConfig
        self.session.open(EPGImportConfig)

    def purge_m3u_bouquets(self, directory="/etc/enigma2", pattern="_m3ubouquet.tv"):
        """Remove all bouquet files created by M3UConverter with correct pattern"""
        create_backup()
        removed_files = []

        for f in listdir(directory):
            file_path = join(directory, f)
            if isfile(file_path) and f.endswith(pattern):
                try:
                    remove(file_path)
                    removed_files.append(f)

                    bouquet_name = f.replace('userbouquet.', '').replace('.tv', '')
                    self._remove_epg_files(bouquet_name)

                except Exception as e:
                    logger.error("Failed to remove %s: %s", f, str(e))

        # Clean the main bouquets.tv file
        self._clean_bouquets_file(directory, pattern)

        # Clean EPG sources file if empty
        self._clean_epg_sources()

        if removed_files:
            message = "Removed %d bouquet(s):\n%s" % (
                len(removed_files),
                "\n".join(removed_files)
            )
        else:
            message = "No M3UConverter bouquets found to remove."

        self.session.open(MessageBox, message, MessageBox.TYPE_INFO, timeout=6)

    def _clean_bouquets_file(self, directory, pattern):
        """Clean the bouquets.tv file by removing references to deleted bouquets"""
        bouquets_file = join(directory, "bouquets.tv")
        if not exists(bouquets_file):
            return

        try:
            with open(bouquets_file, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Keep only lines that don't reference the pattern we're removing
            new_lines = []
            for line in lines:
                if pattern in line:
                    # This line references a bouquet we're removing, skip it
                    continue
                new_lines.append(line)

            with open(bouquets_file, "w", encoding="utf-8") as f:
                f.writelines(new_lines)

        except Exception as e:
            logger.error("Error cleaning bouquets.tv: %s", str(e))

    def _clean_epg_sources(self):
        """Clean the sources.xml file and remove it if empty"""
        epgimport_path = "/etc/epgimport"
        sources_file = join(epgimport_path, "ArchimedeConverter.sources.xml")

        if not fileExists(sources_file):
            return

        try:
            with open(sources_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Remove sources for non-existing bouquets
            pattern = r'<source type="gen_xmltv"[^>]*channels="(.*?)\.channels\.xml"[^>]*>.*?</source>'

            def should_keep_source(match):
                bouquet_name = match.group(1)
                channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
                return fileExists(channels_file)

            # Remove only sources for bouquets that no longer exist
            new_content = sub(pattern, lambda m: m.group(0) if should_keep_source(m) else '', content, flags=DOTALL)

            # Remove empty sourcecat entries
            new_content = sub(r'<sourcecat[^>]*>\s*</sourcecat>', '', new_content)

            # Check if the file has any meaningful content left
            stripped_content = new_content.strip()
            if (not stripped_content or
                stripped_content == '<?xml version="1.0" encoding="utf-8"?><sources></sources>' or
                    'Archimede Converter by Lululla' not in stripped_content):
                # Remove the empty file
                remove(sources_file)
                logger.info("Removed empty EPG sources file")
            else:
                # Write back the cleaned content
                with open(sources_file, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                logger.info("Updated EPG sources file")

        except Exception as e:
            logger.error("Error cleaning EPG sources: %s", str(e))

    def _remove_epg_files(self, bouquet_name):
        """Remove EPG files associated with the bouquet"""
        epgimport_path = "/etc/epgimport"

        # Remove channels.xml file
        channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
        if fileExists(channels_file):
            try:
                remove(channels_file)
                logger.info("Removed EPG channels file: %s", channels_file)
            except Exception as e:
                logger.error("Error removing EPG channels file %s: %s", channels_file, str(e))

        # Remove any .tv.epg.imported file (cache file)
        epg_imported_file = join(epgimport_path, f"{bouquet_name}.tv.epg.imported")
        if fileExists(epg_imported_file):
            try:
                remove(epg_imported_file)
                logger.info("Removed EPG cache file: %s", epg_imported_file)
            except Exception as e:
                logger.error("Error removing EPG cache file %s: %s", epg_imported_file, str(e))

    def selectionMade(self):
        selection = self["list"].getCurrent()
        if selection:
            conversion_type = selection[1]
            if conversion_type == "purge_m3u_bouquets":
                self.purge_m3u_bouquets()
            else:
                self.open_file_browser(conversion_type)

    def open_file_browser(self, conversion_type):
        patterns = {
            "m3u_to_tv": r"(?i)^.*\.(m3u|m3u8)$",
            "tv_to_m3u": r"(?i)^.*\.tv$",
            "json_to_tv": r"(?i)^.*\.json$",
            "json_to_m3u": r"(?i)^.*\.json$",
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
                "json_to_m3u": _("JSON to M3U Conversion"),
                "xspf_to_m3u": _("XSPF to M3U Playlist Conversion"),
                "m3u_to_json": _("M3U to JSON Conversion")
            }

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
            self.session.open(MessageBox, _("fileSelected Error: selection"), MessageBox.TYPE_ERROR, timeout=6)

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


class UniversalConverter(Screen):
    if screen_width > 1280:

        skin = """
        <screen name="UniversalConverter" position="center,center" size="1920,1080" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1920,1080" zPosition="-2" />
            <widget name="list" position="65,70" size="1122,797" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="65,920" size="1127,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="65,880" size="1125,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="black" />
            <widget source="progress_text" render="Label" position="65,880" size="1124,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1200,810" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1200,865" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1200,920" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="1220,125" size="640,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="1220,166" zPosition="20" size="640,360" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="65,1035" size="280,6" zPosition="12" />
            <widget name="key_red" position="65,990" size="280,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="65,990" size="280,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="365,1035" size="280,6" zPosition="12" />
            <widget name="key_green" position="365,990" size="280,45" zPosition="11" font="Regular; 28" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="364,990" size="280,45" zPosition="11" font="Regular; 28" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="666,1035" size="280,6" zPosition="12" />
            <widget name="key_yellow" position="664,990" size="280,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="664,990" size="280,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="968,1035" size="280,6" zPosition="12" />
            <widget name="key_blue" position="967,990" size="280,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="967,990" size="280,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    else:
        skin = """
        <screen name="UniversalConverter" position="center,center" size="1280,720" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="24,616" size="1185,42" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="25,582" size="840,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="black" />
            <widget source="progress_text" render="Label" position="25,582" size="840,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1111,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1165,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1220,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="25,700" size="250,6" zPosition="12" />
            <widget name="key_red" position="25,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="25,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="280,700" size="250,6" zPosition="12" />
            <widget name="key_green" position="280,660" size="250,45" zPosition="11" font="Regular; 28" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="280,660" size="250,45" zPosition="11" font="Regular; 28" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="541,700" size="250,6" zPosition="12" />
            <widget name="key_yellow" position="539,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="539,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="798,700" size="250,6" zPosition="12" />
            <widget name="key_blue" position="797,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="797,660" size="250,45" zPosition="11" font="Regular; 28" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
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
        self.epg_mapper = None
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

        self.onLayoutFinish.append(self.initialize_epg_mapper)

    def initialize_epg_mapper(self):
        """Initialize or reuse the EPG mapper with persistent cache"""
        if self.epg_mapper is None:
            self.epg_mapper = EPGServiceMapper(prefer_satellite=True)
            if not self.epg_mapper.initialize():
                logger.warning("EPGServiceMapper initialization failed")
            else:
                logger.info("EPGServiceMapper initialized with fresh instance")
        else:
            logger.info("Reusing existing EPGServiceMapper instance")

        return self.epg_mapper

    def blue_button_action(self):
        """Dynamic handling of the blue button based on the current state"""
        if self.is_converting:
            self.cancel_convert()
        else:
            self.show_tools_menu()

    def start_conversion_after_show(self):
        """Automatically start the conversion after the screen has been displayed"""
        try:
            self.onShown.remove(self.start_conversion_after_show)
            if self.auto_start and self.selected_file:
                self.start_timer = eTimer()
                self.start_timer.callback.append(self.delayed_start)
                self.start_timer.start(2000)  # 2 second delay
        except:
            pass

    def delayed_start(self):
        """Start the conversion with a slight delay"""
        try:
            self.start_timer.stop()
            self.start_conversion()
        except Exception as e:
            logger.error(f"Error in delayed_start: {str(e)}")

    def create_manual_backup(self):
        try:
            self.converter._create_backup()
            self.session.open(MessageBox, _("Backup created successfully!"), MessageBox.TYPE_INFO, timeout=6)
        except Exception as e:
            self.session.open(MessageBox, _(f"Backup failed: {str(e)}"), MessageBox.TYPE_ERROR, timeout=6)

    def init_tv_converter(self):
        self.update_path_tv()

    def update_path_tv(self):
        try:
            if not exists("/etc/enigma2"):
                raise OSError("Bouquets path not found")

            if not access("/etc/enigma2", W_OK):
                logger.warning("Read-only bouquets path /etc/enigma2")

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"TV path error: {str(e)}")
            self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR, timeout=6)

    def open_file(self):
        """The ONLY way to manage file browser opening"""
        logger.debug(f"Opening file browser for {self.conversion_type}")

        try:
            path = "/etc/enigma2" if self.conversion_type == "tv_to_m3u" else config.plugins.m3uconverter.lastdir.value
            pattern = r"(?i)^.*\.(tv|m3u|m3u8|json|xspf)$"
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
                timeout=6
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

    def start_conversion(self):
        if self.is_converting:
            return

        if not hasattr(self, 'selected_file') or not self.selected_file:
            self.session.open(MessageBox, _("No file selected for conversion"), MessageBox.TYPE_WARNING)
            return

        # Update text based on conversion type
        conversion_texts = {
            "m3u_to_tv": _("Converting to TV"),
            "tv_to_m3u": _("Converting to M3U"),
            "json_to_tv": _("Converting JSON to TV"),
            "json_to_m3u": _("Converting JSON to M3U"),
            "xspf_to_m3u": _("Converting XSPF to M3U"),
            "m3u_to_json": _("Converting M3U to JSON")
        }

        green_text = conversion_texts.get(self.conversion_type, _("Converting"))
        self["key_green"].setText(green_text)
        self["status"].setText(_("Conversion in progress..."))

        # Handle different conversion types
        if self.conversion_type == "m3u_to_tv":
            self.convert_m3u_to_tv()
        elif self.conversion_type == "tv_to_m3u":
            self.convert_tv_to_m3u()
        elif self.conversion_type == "json_to_tv":
            self.convert_json_to_tv()
        elif self.conversion_type == "json_to_m3u":
            self.convert_json_to_m3u()
        elif self.conversion_type == "xspf_to_m3u":
            self.convert_xspf_to_m3u()
        elif self.conversion_type == "m3u_to_json":
            self.convert_m3u_to_json()
        else:
            self.session.open(MessageBox, _("Unsupported conversion type"), MessageBox.TYPE_ERROR)

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
                elif self.conversion_type == "m3u_to_json":
                    self.parse_m3u(selected_path)
                elif self.conversion_type == "json_to_m3u":
                    self.parse_json(selected_path)
                elif self.conversion_type == "json_to_tv":
                    self.parse_json(selected_path)
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

    def _update_ui_success(self, channel_count):
        """Update UI only if necessary"""
        if not hasattr(self, '_last_channel_count') or self._last_channel_count != channel_count:
            self._last_channel_count = channel_count

            conversion_ready_texts = {
                "m3u_to_tv": _("Ready to convert to TV"),
                "tv_to_m3u": _("Ready to convert to M3U"),
                "json_to_tv": _("Ready to convert JSON to TV"),
                "json_to_m3u": _("Ready to convert JSON to M3U"),
                "xspf_to_m3u": _("Ready to convert XSPF to M3U"),
                "m3u_to_json": _("Ready to convert M3U to JSON")
            }

            ready_text = conversion_ready_texts.get(self.conversion_type, _("Ready to convert"))
            self["key_green"].setText(ready_text)
            self["status"].setText(_("Loaded %d channels. Press Green to convert.") % channel_count)

    def process_url(self, url):
        """Process URLs based on settings"""
        url = url.replace(":", "%3a")
        if config.plugins.m3uconverter.hls_convert.value:
            if any(url.lower().endswith(x) for x in ('.m3u8', '.stream')):
                url = f"hls://{url}"
        return url

    def write_group_bouquet(self, group, channels):
        """
        Writes a bouquet file for a single group safely and efficiently,
        with improved encoding handling.
        """
        try:
            safe_name = self.get_safe_filename(group)
            filename = join("/etc/enigma2", "userbouquet." + safe_name + ".tv")
            temp_file = filename + ".tmp"

            name_bouquet = self.remove_suffixes(group)
            name_bouquet = clean_group_name(name_bouquet)

            content_lines = []
            content_lines.append("#NAME " + name_bouquet + "\n")
            content_lines.append("#SERVICE 1:64:0:0:0:0:0:0:0:0::--- | Archimede Converter | ---\n")
            content_lines.append("#DESCRIPTION --- | Archimede Converter | ---\n")

            for ch in channels:
                content_lines.append("#SERVICE " + ch["url"] + "\n")
                desc = ch["name"]
                desc = ''.join(c for c in desc if c.isprintable() or c.isspace())
                desc = transliterate(desc)
                content_lines.append("#DESCRIPTION " + desc + "\n")

            # Scrittura unica
            with open(temp_file, "w", encoding="utf-8") as f:
                f.writelines(content_lines)

            replace(temp_file, filename)
            chmod(filename, 0o644)

        except Exception as e:
            if exists(temp_file):
                try:
                    remove(temp_file)
                except Exception as cleanup_error:
                    logger.error(f"Cleanup error: {str(cleanup_error)}")

            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Failed to write bouquet {str(group)} : {str(e)}")
            raise RuntimeError("Bouquet creation failed for " + group) from e

    def remove_suffixes(self, name):
        """Remove all known suffixes from the name for display purposes"""
        suffixes = ['_m3ubouquet', '_bouquet', '_iptv', '_tv']

        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break  # Rimuovi solo il primo suffisso trovato

        return name

    def get_safe_filename(self, name):
        """Generate a secure file name for bouquets with duplicate suffixes"""
        for suffix in ['_m3ubouquet', '_bouquet', '_iptv', '_tv']:
            if name.endswith(suffix):
                name = name[:-len(suffix)]

        normalized = unicodedata.normalize("NFKD", name)
        safe_name = normalized.encode('ascii', 'ignore').decode('ascii')
        safe_name = sub(r'[^a-zA-Z0-9_-]', '_', safe_name)
        safe_name = sub(r'_+', '_', safe_name).strip('_')

        suffix = "_m3ubouquet"
        base_name = safe_name[:50 - len(suffix)] if len(safe_name) > 50 - len(suffix) else safe_name

        return base_name + suffix if base_name else "m3uconverter_bouquet"

    def _get_output_filename(self):
        """Generate a unique file name for export"""
        timestamp = strftime("%Y%m%d_%H%M%S")
        return f"{archimede_converter_path}/archimede_export_{timestamp}.m3u"

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

    def filter_binary_data(self, channels):
        """Filter out channels with binary data in names or URLs"""
        filtered_channels = []

        for channel in channels:
            # Check channel name
            name = channel.get('name', '')
            if not self._is_valid_text(name):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("Skipping channel with binary name: %s", name[:50] + "..." if len(name) > 50 else name)
                continue

            # Check group name
            group = channel.get('group', '')
            group = clean_group_name(group)
            if group and not self._is_valid_text(group):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("Skipping channel with binary group: %s", group[:50] + "..." if len(group) > 50 else group)
                continue

            # Check URL
            url = channel.get('url', '')
            if not url.startswith(('http://', 'https://', 'rtsp://', 'rtmp://', 'udp://', 'rtp://', '4097:')):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("Skipping channel with invalid URL: %s", url[:50] + "..." if len(url) > 50 else url)
                continue

            filtered_channels.append(channel)

        return filtered_channels

    def handle_very_large_file(self, filename):
        """Handle extremely large M3U files with sampling"""
        self["status"].setText(_("Very large file detected. Sampling first 1000 channels..."))

        entries = []
        count = 0
        max_channels = 1000  # Limit for very large files

        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                current_params = {}
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    if count >= max_channels:
                        break

                    if line.startswith('#EXTINF:'):
                        current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                        parts = line[8:].split(',', 1)
                        if len(parts) > 1:
                            current_params['title'] = parts[1].strip()

                    elif line.startswith('#EXTGRP:'):
                        current_params['group-title'] = line[8:].strip()

                    elif line.startswith('#'):
                        continue

                    else:
                        if current_params and line:
                            current_params['uri'] = line.strip()
                            if current_params.get('title'):
                                entries.append(current_params.copy())
                                count += 1
                            current_params = {}

        except Exception as e:
            logger.error(f"Large file sampling error: {str(e)}")

        return entries

    def parse_m3u(self, filename=None):
        """M3U parsing with improved attribute handling"""
        try:
            file_to_parse = filename or self.selected_file
            if not file_to_parse:
                raise ValueError(_("No file specified"))

            file_size = getsize(file_to_parse)

            # Use appropriate parsing method based on file size
            if file_size > 10 * 1024 * 1024:  # > 10MB
                self.m3u_list = self.handle_very_large_file(file_to_parse)
            elif file_size > 1 * 1024 * 1024:  # > 1MB
                self.m3u_list = self._parse_m3u_incremental(file_to_parse)
            else:
                with open(file_to_parse, 'r', encoding='utf-8', errors='replace') as f:
                    data = f.read()
                self.m3u_list = self._parse_m3u_content(data)

            # Filter and process channels with proper attribute mapping
            filtered_channels = []
            for channel in self.m3u_list:
                if channel.get('uri'):
                    # Map attributes to consistent names
                    filtered_channels.append({
                        'name': channel.get('title', ''),
                        'url': self.process_url(channel['uri']),
                        'group': channel.get('group-title', ''),
                        'tvg_id': channel.get('tvg-id', ''),
                        'tvg_name': channel.get('tvg-name', ''),
                        'logo': channel.get('tvg-logo', ''),
                        'duration': channel.get('length', '-1'),
                        'user_agent': channel.get('user-agent', ''),
                        'language': channel.get('tvg-language', '')
                    })

            self.m3u_list = filtered_channels

            # Update UI
            display_list = []
            for idx, channel in enumerate(self.m3u_list[:100]):  # Show only first 100
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', 'Default')
                group = clean_group_name(group)
                display_list.append(f"{idx + 1:03d}. {group + ' - ' if group else ''}{name}")

            self["list"].setList(display_list)
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_list))

        except Exception as e:
            logger.error(f"Error parsing M3U: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            self.session.open(
                MessageBox,
                _("Error parsing file. File may be too large or corrupt."),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _parse_m3u_content(self, data):
        """Advanced parser for M3U content with support for extended attributes"""
        entries = []
        current_params = {}
        lines_processed = 0
        # self.epg_mapper = self.initialize_epg_mapper()
        # self.epg_mapper.clear_match_cache_only()
        # Process in chunks to avoid memory issues
        lines = data.split('\n')

        for line in lines:
            lines_processed += 1
            line = line.strip()
            if not line:
                continue

            # Periodically yield control to avoid UI freeze
            if lines_processed % 100 == 0:
                from enigma import eTimer
                eTimer().start(10, True)

            if line.startswith('#EXTM3U'):
                continue  # Skip header

            elif line.startswith('#EXTINF:'):
                current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                # Extract the entire EXTINF line after the colon
                extinf_content = line[8:].strip()

                # Split into attributes and title
                if ',' in extinf_content:
                    # Find the last comma which separates attributes from title
                    last_comma_index = extinf_content.rfind(',')
                    attributes_part = extinf_content[:last_comma_index].strip()
                    title_part = extinf_content[last_comma_index + 1:].strip()

                    current_params['title'] = title_part
                    """
                    # Cerca tvg_id nel nome del canale
                    # tvg_id_from_name = self.epg_mapper._extract_tvg_id_from_name(title_part)
                    # if tvg_id_from_name and 'tvg-id' not in current_params:
                        # current_params['tvg-id'] = tvg_id_from_name
                    """
                    # Parse attributes with key="value" format
                    attributes = {}
                    # Find all key="value" pairs
                    attr_matches = findall(r'(\S+?)="([^"]*)"', attributes_part)
                    for key, value in attr_matches:
                        attributes[key.lower()] = value

                    # Also check for duration (number at the beginning)
                    duration_match = search(r'^(-?\d+)', attributes_part)
                    if duration_match:
                        attributes['length'] = duration_match.group(1)

                    current_params.update(attributes)

                else:
                    # Fallback: if no comma, use entire content as title
                    current_params['title'] = extinf_content

                common_attributes = ['tvg-id', 'tvg-name', 'tvg-logo', 'group-title', 'tvg-language']
                for attr in common_attributes:
                    if attr in attributes:
                        current_params[attr] = attributes[attr]

            elif line.startswith('#EXTGRP:'):
                current_params['group-title'] = line[8:].strip()

            elif line.startswith('#EXTVLCOPT:'):
                opt_line = line[11:].strip()
                if '=' in opt_line:
                    key, value = opt_line.split('=', 1)
                    key = key.lower().strip()
                    if key == 'http-user-agent':
                        current_params['user-agent'] = value.strip()

            elif line.startswith('#'):
                continue  # Skip other comments

            else:
                # URL line
                if current_params and line and not line.startswith('#'):
                    current_params['uri'] = line.strip()
                    if current_params.get('title') and current_params.get('uri'):
                        entries.append(current_params)
                    current_params = {}

        return entries

    def _parse_m3u_incremental(self, filename, chunk_size=8192):
        """Parse M3U file incrementally with attribute support"""
        entries = []
        current_params = {}
        buffer = ""

        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    buffer += chunk
                    lines = buffer.split('\n')

                    # Keep last incomplete line in buffer
                    if buffer and not buffer.endswith('\n'):
                        buffer = lines.pop()
                    else:
                        buffer = ""

                    # Process complete lines
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue

                        # Yield control periodically
                        if len(entries) % 50 == 0:
                            from enigma import eTimer
                            eTimer().start(5, True)

                        if line.startswith('#EXTINF:'):
                            current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                            extinf_content = line[8:].strip()

                            if ',' in extinf_content:
                                last_comma_index = extinf_content.rfind(',')
                                attributes_part = extinf_content[:last_comma_index].strip()
                                title_part = extinf_content[last_comma_index + 1:].strip()

                                current_params['title'] = title_part

                                # Parse attributes
                                attr_matches = findall(r'(\S+?)="([^"]*)"', attributes_part)
                                for key, value in attr_matches:
                                    current_params[key.lower()] = value

                                # Get duration
                                duration_match = search(r'^(-?\d+)', attributes_part)
                                if duration_match:
                                    current_params['length'] = duration_match.group(1)
                            else:
                                current_params['title'] = extinf_content

                        elif line.startswith('#EXTGRP:'):
                            current_params['group-title'] = line[8:].strip()

                        elif line.startswith('#'):
                            continue

                        else:
                            if current_params and line:
                                current_params['uri'] = line.strip()
                                if current_params.get('title'):
                                    entries.append(current_params.copy())
                                current_params = {}

        except Exception as e:
            logger.error(f"Incremental parsing error: {str(e)}")

        return entries

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

            self.m3u_list = channels
            self["list"].setList([c[0] for c in channels])
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_list))
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
            channels = []
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"JSON data type: {type(data)}")
            if isinstance(data, dict):
                logger.debug(f"JSON keys: {list(data.keys())}")

            # Handle different JSON structures
            if isinstance(data, dict):
                # Try various common JSON structures
                if 'channels' in data and isinstance(data['channels'], list):
                    channels = data['channels']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'channels' key")
                elif 'playlist' in data and isinstance(data['playlist'], list):
                    channels = data['playlist']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'playlist' key")
                elif 'items' in data and isinstance(data['items'], list):
                    channels = data['items']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'items' key")
                elif 'streams' in data and isinstance(data['streams'], list):
                    channels = data['streams']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'streams' key")
                elif 'data' in data and isinstance(data['data'], list):
                    channels = data['data']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'data' key")
                else:
                    # If no specific key found, try to use all values that are lists
                    for key, value in data.items():
                        if isinstance(value, list):
                            channels = value
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.debug(f"Using list from key: {key}")
                            break

            elif isinstance(data, list):
                # Direct array of channels
                channels = data
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("JSON is direct array of channels")
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Unsupported JSON structure")
                raise ValueError("Unsupported JSON structure")

            # Process channels
            for channel in channels:
                if not isinstance(channel, dict):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Skipping non-dict channel: {channel}")
                    continue

                # Extract channel information with flexible field names
                name = (channel.get('name') or channel.get('title') or
                        channel.get('channel') or channel.get('channel_name') or 'Unknown')

                url = (channel.get('url') or channel.get('link') or channel.get('stream') or
                       channel.get('source') or channel.get('address') or '')

                # Decode URL if it's encoded (check for % encoding)
                if url and '%' in url:
                    try:
                        url = unquote(url)
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Decoded URL: {url}")
                    except Exception as e:
                        logger.error(f"Error decoding URL: {str(e)}")

                group = (channel.get('group') or channel.get('category') or
                         channel.get('group-title') or channel.get('group_title') or '')

                logo = (channel.get('logo') or channel.get('icon') or
                        channel.get('tvg-logo') or channel.get('tvg_logo') or '')

                tvg_id = (channel.get('tvg-id') or channel.get('tvg_id') or
                          channel.get('id') or channel.get('channel_id') or '')

                tvg_name = channel.get('tvg-name') or channel.get('tvg_name') or name

                # Check if URL is valid after decoding
                is_valid_url = False
                if url:
                    # Check for various URL protocols
                    url_protocols = ('http://', 'https://', 'rtsp://', 'rtmp://', 'udp://', 'rtp://')
                    is_valid_url = any(url.startswith(proto) for proto in url_protocols)

                    # Also check for encoded URLs that might become valid after decoding
                    if not is_valid_url and '%' in url:
                        decoded_url = unquote(url)
                        is_valid_url = any(decoded_url.startswith(proto) for proto in url_protocols)
                        if is_valid_url:
                            url = decoded_url

                group = clean_group_name(group)
                # Only add channels with a valid URL
                if is_valid_url:
                    self.m3u_list.append({
                        'name': name,
                        'url': url,
                        'group': group,
                        'logo': logo,
                        'tvg_id': tvg_id,
                        'tvg_name': tvg_name,
                        'duration': channel.get('duration', '-1'),
                        'user_agent': channel.get('user-agent') or channel.get('user_agent') or '',
                        'program_id': channel.get('program-id') or channel.get('program_id') or ''
                    })
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"Added channel: {name} - {url}")
                else:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Skipping channel without valid URL: {name} - URL: {url}")

            # Update UI based on conversion type
            display_list = []
            for channel in self.m3u_list:
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', '')
                display_list.append(f"{group + ' - ' if group else ''}{name}")

            # Update the list immediately
            self["list"].setList(display_list)
            self.file_loaded = True

            self._update_ui_success(len(self.m3u_list))

            # Log results for debugging
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Found {len(self.m3u_list)} channels in JSON file")
            if len(self.m3u_list) > 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Sample channel: {self.m3u_list[0]}")

        except Exception as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            self.file_loaded = False
            self.m3u_list = []
            self.session.open(
                MessageBox,
                _("Error parsing JSON file. Please check the format.\n\nError: %s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _convert_m3u_to_tv_task(self, m3u_path, progress_callback):
        """Task for converting M3U to TV bouquet"""
        # self.epg_mapper = self.initialize_epg_mapper()
        # self.epg_mapper.clear_match_cache_only()

        try:
            # Initial cancellation check
            if self.cancel_conversion:
                return (False, "Conversion cancelled")

            # Extract EPG URL from the M3U file
            epg_url = self.epg_mapper.extract_epg_url_from_m3u(m3u_path)

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
                if config.plugins.m3uconverter.enable_debug.value:
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

                # satellite = epg_mapper.get_satellite_for_channel_id(channel.get('tvg_id', ''))
                # if satellite:
                    # logger.debug(f"📡 Channel {channel['name']} is on satellite {satellite}")
                # else:
                    # logger.debug(f"❓ Channel {channel['name']} has no satellite mapping")
                clean_name = self.epg_mapper.clean_channel_name(channel['name'])
                tvg_id = channel.get('tvg_id')

                if idx % 100 == 0:  # Log ogni 100 canali invece di tutti
                    logger.info("Processed %d/%d channels", idx, total_channels)

                # Get the best service reference for EPG
                sref, match_type = self.epg_mapper.find_best_service_match(
                    clean_name,
                    tvg_id,
                    channel['name']
                )

                # Inizializza le variabili per evitare UnboundLocalError
                hybrid_sref = None
                epg_sref = None

                if sref:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found service reference for %s: %s (match type: %s)", channel['name'], sref, match_type)
                    # Use hybrid service reference for bouquet
                    hybrid_sref = self.epg_mapper.generate_hybrid_sref(sref, channel['url'])
                    # For EPG, we need the DVB service reference (without URL)
                    epg_sref = self.epg_mapper.generate_hybrid_sref(sref, for_epg=True)

                    # Add to EPG data
                    epg_data.append({
                        'tvg_id': tvg_id or channel['name'],
                        'sref': epg_sref,
                        'name': channel['name']
                    })

                    # Use the hybrid reference for the bouquet
                    channel['url'] = hybrid_sref
                else:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("No service reference found for %s, using standard IPTV reference", channel['name'])
                    # Fallback to standard IPTV reference
                    channel['url'] = self.epg_mapper.generate_service_reference(channel['url'])

                group = clean_group_name(channel.get('group', 'Default'))
                groups.setdefault(group, []).append(channel)
                # Aggiorna progresso ogni 50 canali
                if idx % 50 == 0:
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
                    group = clean_group_name(group)
                    safe_group_name = self.get_safe_filename(group)
                    self.write_group_bouquet(safe_group_name, channels)
                    self.update_progress(
                        total_channels + group_idx,
                        _("Creating bouquet: %s") % safe_group_name
                    )

            # Phase 3: Update main bouquet
            if config.plugins.m3uconverter.bouquet_mode.value == "single":
                self.update_main_bouquet([bouquet_name])
            else:
                self.update_main_bouquet([self.get_safe_filename(group) for group in groups.keys()])

            # Phase 4: Generate EPG files if data is available
            if epg_data and config.plugins.m3uconverter.epg_enabled.value:
                channels_success = self.epg_mapper.generate_epg_channels_file(epg_data, bouquet_name)

                # USA generate_epg_sources_file2 (con sorgenti complete)
                sources_success = self.epg_mapper.generate_epg_sources_file2(bouquet_name, epg_url)

                if channels_success and sources_success:
                    logger.info("Generated EPG files for %d channels", len(epg_data))

            cache_stats = self.epg_mapper.get_cache_stats()
            self.show_conversion_stats(total_channels, len(epg_data), cache_stats)
            return (True, total_channels, len(epg_data))

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error in M3U to TV conversion: {str(e)}")
            return (False, str(e))

    def _real_conversion_task(self):
        try:
            # Extract EPG URL from M3U file
            epg_url = self.epg_mapper.extract_epg_url_from_m3u(self.selected_file)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("Extracted EPG URL: %s", epg_url)

            groups = {}
            epg_data = []
            total_channels = len(self.m3u_list)

            # Phase 1: Channel Grouping with EPG mapping
            for idx, channel in enumerate(self.m3u_list):
                if not channel.get('url'):  # Skip channels without URLs
                    continue

                clean_name = self.epg_mapper.clean_channel_name(channel['name'])
                tvg_id = channel.get('tvg_id')
                # # ⭐⭐⭐ ⭐⭐⭐
                """
                # tvg_id = self.epg_mapper.get_satellite_for_channel_id(channel.get('tvg_id', ''))
                # if tvg_id:
                    # logger.debug(f"📡 Channel {channel['name']} is on satellite {tvg_id}")
                # else:
                    # logger.debug(f"❓ Channel {channel['name']} has no satellite mapping")
                """
                if idx % 100 == 0:  # Log ogni 100 canali invece di tutti
                    logger.info("Processed %d/%d channels", idx, total_channels)

                # Get the best service reference for EPG
                # sref, match_type = epg_mapper.find_best_service_match(clean_name, tvg_id)
                sref, match_type = self.epg_mapper.find_best_service_match(
                    clean_name,
                    tvg_id,
                    channel['name']
                )

                # Initialize variables to avoid UnboundLocalError
                hybrid_sref = None
                epg_sref = None

                if sref:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found service reference for %s: %s (match type: %s)", channel['name'], sref, match_type)
                    # Use hybrid service reference for bouquet
                    hybrid_sref = self.epg_mapper.generate_hybrid_sref(sref, channel['url'])
                    # For EPG, we need the DVB service reference (without URL)
                    epg_sref = self.epg_mapper.generate_hybrid_sref(sref, for_epg=True)

                    # Add to EPG data
                    epg_data.append({
                        'tvg_id': tvg_id or channel['name'],
                        'sref': epg_sref,
                        'name': channel['name']
                    })

                    # Use the hybrid reference for the bouquet
                    channel['url'] = hybrid_sref
                else:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("No service reference found for %s, using standard IPTV reference", channel['name'])
                    # Fallback to standard IPTV reference
                    channel['url'] = self.epg_mapper.generate_service_reference(channel['url'])

                # # Secure logging - only if variables are defined
                # logger.debug("Original URL: %s", channel['url'])
                # if hybrid_sref:
                    # logger.debug("Generated service reference for bouquet: %s", hybrid_sref)
                # if epg_sref:
                    # logger.debug("Generated service reference for EPG: %s", epg_sref)

                group = clean_group_name(channel.get('group', 'Default'))
                groups.setdefault(group, []).append(channel)
                # Aggiorna progresso ogni 50 canali
                if idx % 50 == 0:
                    progress = (idx + 1) / total_channels * 100
                    # self.update_progress(idx + 1, _("Processing: %d%%") % progress)
                    name = str(channel.get("name") or "--")
                    self.update_progress(idx + 1, _("Processing: %s (%d%%)") % (name, progress))

                # progress = (idx + 1) / total_channels * 100
                # name = str(channel.get("name") or "--")
                # self.update_progress(idx + 1, _("Processing: %s (%d%%)") % (name, progress))

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

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("Generating EPG files for bouquet: %s", bouquet_name_for_epg)
                    logger.debug("EPG data for %d channels", len(epg_data))

                # Generate channels.xml
                channels_success = self.epg_mapper.generate_epg_channels_file(epg_data, bouquet_name_for_epg)
                if not channels_success:
                    logger.error("Failed to generate EPG channels file")

                # Generate sources.xml
                sources_success = self.epg_mapper.generate_epg_sources_file2(bouquet_name_for_epg, epg_url)

                if channels_success and sources_success:
                    logger.info("EPG files generated successfully for %d channels", len(epg_data))
                else:
                    logger.warning("Failed to generate some EPG files")

            else:
                logger.warning("No EPG data to generate files or EPG disabled")

            cache_stats = self.epg_mapper.get_cache_stats()
            stats_data = {
                'total_channels': total_channels,
                'epg_channels': len(epg_data),
                'hit_rate': cache_stats['hit_rate'],
                'cache_size': cache_stats['cache_size'],
                'compatible_cache': cache_stats['cache_analysis'].get('compatible', 0),
                'incompatible_matches': cache_stats['incompatible_matches'],
                'rytec_channels': cache_stats['rytec_channels']
            }

            self.show_conversion_stats("m3u_to_tv", stats_data)
            return (True, total_channels, len(epg_data))
        except Exception as e:
            logger.exception(f"Error during real conversion {str(e)}")
            raise

    def convert_m3u_to_tv(self):
        # Initialize EPG mapper only once
        # self.epg_mapper = self.initialize_epg_mapper()
        # self.epg_mapper.clear_match_cache_only()

        sky_in_rytec = self.epg_mapper.debug_rytec_content("sky")
        if isinstance(sky_in_rytec, list):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("Sky channels in rytec:")
            for sky_channel in sky_in_rytec[:20]:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"   {sky_channel}")
        else:
            logger.debug(f"{sky_in_rytec}")

        def conversion_task():
            try:
                return self.converter.safe_convert(self._real_conversion_task)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
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

                    file_size = getsize(output_file) if exists(output_file) else 0

                    stats_data = {
                        'total_channels': total_items,
                        'output_file': basename(output_file),
                        'file_size': file_size
                    }
                    self.show_conversion_stats("tv_to_m3u", stats_data)
                return (True, total_items, output_file)
            except IOError as e:
                raise RuntimeError(_("File write error: %s") % str(e))
            except Exception as e:
                raise RuntimeError(_("tv_to_m3u Conversion error: %s") % str(e))

        def conversion_task():
            try:
                return self.converter.safe_convert(_real_tv_to_m3u_conversion)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"TV-to-M3U conversion failed: {str(e)}")
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def convert_json_to_m3u(self):
        """Convert JSON to M3U format"""
        def _json_to_m3u_conversion():
            try:
                # Parse the JSON file if not already parsed
                if not self.m3u_list:
                    self.parse_json(self.selected_file)

                if not self.m3u_list:
                    raise ValueError("No valid channels found in JSON file")

                total_channels = len(self.m3u_list)
                base_name = basename(self.selected_file).split('.')[0]
                output_dir = dirname(self.selected_file)
                output_file = join(output_dir, f"{base_name}.m3u")

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    for idx, channel in enumerate(self.m3u_list):
                        # Update progress
                        progress = (idx + 1) / total_channels * 100
                        self.update_progress(
                            idx + 1,
                            _("Converting: %s (%d%%)") % (channel.get('name', 'Unknown'), progress)
                        )
                        # Write EXTINF line
                        name = channel.get('name', '')
                        tvg_id = channel.get('tvg_id', '')
                        tvg_name = channel.get('tvg_name', '')
                        tvg_logo = channel.get('logo', '')
                        group = channel.get('group', '')
                        group = clean_group_name(group)
                        duration = channel.get('duration', '-1')

                        extinf = f'#EXTINF:{duration}'
                        if tvg_id:
                            extinf += f' tvg-id="{tvg_id}"'
                        if tvg_name:
                            extinf += f' tvg-name="{tvg_name}"'
                        if tvg_logo:
                            extinf += f' tvg-logo="{tvg_logo}"'
                        if group:
                            extinf += f' group-title="{group}"'
                        extinf += f',{name}\n'

                        f.write(extinf)
                        f.write(channel['url'] + '\n')

                # Calculate file size
                file_size = getsize(output_file) if exists(output_file) else 0

                stats_data = {
                    'total_channels': total_channels,
                    'output_file': basename(output_file),
                    'file_size': file_size
                }

                self.show_conversion_stats("json_to_m3u", stats_data)
                return (True, output_file, total_channels)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Error converting JSON to M3U: {str(e)}")
                return (False, str(e))

        # Start conversion in a separate thread
        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))

        def conversion_task():
            try:
                return _json_to_m3u_conversion()
            except Exception as e:
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def convert_m3u_to_json(self):
        """Convert M3U playlist to JSON format"""
        def _m3u_to_json_conversion():
            try:
                # Parse the M3U file if not already parsed
                if not self.m3u_list:
                    with open(self.selected_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    self.m3u_list = self._parse_m3u_content(content)

                if not self.m3u_list:
                    raise ValueError("No valid channels found in M3U file")

                # Create JSON structure
                json_data = {"playlist": []}

                for idx, channel in enumerate(self.m3u_list):
                    # Update progress
                    progress = (idx + 1) / len(self.m3u_list) * 100
                    self.update_progress(
                        idx + 1,
                        _("Converting: %s (%d%%)") % (channel.get('title', 'Unknown'), progress)
                    )

                    # Copy all attributes found in parsing
                    channel_data = {}
                    for key, value in channel.items():
                        channel_data[key] = value

                    json_data["playlist"].append(channel_data)

                # Generate output filename
                base_name = basename(self.selected_file).split('.')[0]
                output_dir = dirname(self.selected_file)
                output_file = join(output_dir, f"{base_name}.json")

                # Write JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=4, ensure_ascii=False)

                # Calculate file size and statistics
                file_size = getsize(output_file) if exists(output_file) else 0

                # Determine the JSON structure
                json_structure = "playlist"
                if 'channels' in json_data:
                    json_structure = "channels"
                elif 'items' in json_data:
                    json_structure = "items"
                elif 'streams' in json_data:
                    json_structure = "streams"

                stats_data = {
                    'total_channels': len(json_data.get("playlist", [])),
                    'output_file': basename(output_file),
                    'file_size': file_size,
                    'json_structure': json_structure
                }

                self.show_conversion_stats("m3u_to_json", stats_data)
                return (True, output_file, len(json_data["playlist"]))
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Error converting M3U to JSON: {str(e)}")
                return (False, str(e))

        # Start conversion in thread
        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))

        def conversion_task():
            try:
                return _m3u_to_json_conversion()
            except Exception as e:
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def convert_json_to_tv(self):
        if not self.m3u_list:
            self.parse_json(self.selected_file)

        if not self.m3u_list:
            raise ValueError("No valid channels found in JSON file")

        def _json_tv_conversion():
            try:
                result = self._real_conversion_task()  # It uses the same logic as m3u_to_tv

                if result[0]:  # Success
                    cache_stats = self.epg_mapper.get_cache_stats()
                    stats_data = {
                        'total_channels': result[1],
                        'epg_channels': result[2],
                        'hit_rate': cache_stats['hit_rate'],
                        'cache_size': cache_stats['cache_size'],
                        'compatible_cache': cache_stats['cache_analysis'].get('compatible', 0),
                        'incompatible_matches': cache_stats['incompatible_matches'],
                        'rytec_channels': cache_stats['rytec_channels']
                    }

                    self.show_conversion_stats("json_to_tv", stats_data)

                return result
            except Exception as e:
                return (False, str(e))

        # Conversion Started
        threads.deferToThread(_json_tv_conversion).addBoth(self.conversion_finished)

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

                track_count = len(root.findall('.//ns:track', ns))
                file_size = getsize(output_file) if exists(output_file) else 0

                stats_data = {
                    'total_channels': track_count,
                    'output_file': basename(output_file),
                    'file_size': file_size
                }

                self.show_conversion_stats("xspf_to_m3u", stats_data)
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

            self.session.open(MessageBox, msg, MessageBox.TYPE_INFO, timeout=6)
        else:
            self.session.open(MessageBox, _("Conversion failed: %s") % result[1], MessageBox.TYPE_ERROR, timeout=6)

    def conversion_finished(self, result):
        self["progress_source"].setValue(0)
        from twisted.internet import reactor
        reactor.callFromThread(self._show_conversion_result, result)

        if not result[0]:
            self.session.open(
                MessageBox,
                _("Conversion failed: %s") % result[1],
                MessageBox.TYPE_ERROR,
                timeout=6
            )

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
        self.session.open(MessageBox, _("Conversion cancelled"), MessageBox.TYPE_INFO, timeout=6)

    def _conversion_error(self, error_msg):
        """Handle conversion error"""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))  # Ripristina il testo originale
        self.session.open(MessageBox, _("Conversion error: %s") % error_msg, MessageBox.TYPE_ERROR, timeout=6)

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
            if config.plugins.m3uconverter.enable_debug.value:
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
        """Show the conversion result with consistent formatting"""
        try:
            self.is_converting = False
            self.cancel_conversion = False
            self["key_green"].setText(_("Convert"))
            self["key_blue"].setText(_("Tools"))

            if not isinstance(result, tuple):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Invalid result format: {result}")
                self.session.open(MessageBox, _("Conversion completed with unknown result"), MessageBox.TYPE_INFO)
                return

            success = result[0]
            data = result[1:] if len(result) > 1 else []

            if success:
                success_messages = {
                    "m3u_to_tv": _("Successfully converted %d channels to TV bouquets"),
                    "tv_to_m3u": _("Successfully converted %d channels to M3U playlist"),
                    "json_to_tv": _("Successfully converted %d JSON channels to TV bouquets"),
                    "json_to_m3u": _("Successfully converted %d JSON channels to M3U playlist"),
                    "xspf_to_m3u": _("Successfully converted XSPF to M3U playlist"),
                    "m3u_to_json": _("Successfully converted %d M3U channels to JSON")
                }

                base_message = success_messages.get(self.conversion_type, _("Successfully converted %d items"))

                if data:
                    if len(data) >= 2 and isinstance(data[1], int):
                        # Case with channel count
                        channel_count = data[1]
                        message = base_message % channel_count

                        # Add EPG info if available
                        if len(data) >= 3 and isinstance(data[2], int) and data[2] > 0:
                            message += _("\nEPG mapped for %d channels") % data[2]
                            message += _("\nEPG files generated in /etc/epgimport/")

                        # Add file path if available
                        if len(data) >= 1 and isinstance(data[0], str) and exists(data[0]):
                            message += _("\nSaved to: %s") % data[0]
                    else:
                        # Generic case
                        message = _("Conversion completed successfully")
                else:
                    message = _("Conversion completed successfully")

                self.session.open(MessageBox, message, MessageBox.TYPE_INFO, timeout=6)

            else:
                # Error message
                error_msg = data[0] if data else _("Unknown error")
                self.session.open(MessageBox, _("Conversion failed: %s") % error_msg, MessageBox.TYPE_ERROR, timeout=6)

            self["status"].setText(_("Conversion completed"))
            self["progress_text"].setText("")

        except Exception as e:
            logger.error(f"Error showing conversion result: {str(e)}")
            self.session.open(MessageBox, _("Error processing conversion result"), MessageBox.TYPE_ERROR)

    def show_conversion_stats(self, conversion_type, stats_data):
        """Mostra le statistiche di conversione in un popup per tutti i tipi di conversione"""

        stats_message = [_("🎯 CONVERSION COMPLETE")]

        if conversion_type in ["m3u_to_tv", "json_to_tv"]:
            stats_message.extend([
                _("📊 Total channels processed: %d") % stats_data.get('total_channels', 0),
                _("📡 EPG mapped channels: %d") % stats_data.get('epg_channels', 0),
                _("💾 Cache efficiency: %s") % stats_data.get('hit_rate', '0%'),
                _("🔍 Cache size: %d") % stats_data.get('cache_size', 0),
                _("🔍 Compatible in cache: %d") % stats_data.get('compatible_cache', 0),
                _("🔍 Incompatible Matches: %d") % stats_data.get('incompatible_matches', 0),
                _("📊 Rytec channels: %d") % stats_data.get('rytec_channels', 0)
            ])
        elif conversion_type in ["tv_to_m3u", "json_to_m3u", "xspf_to_m3u"]:
            stats_message.extend([
                _("📊 Channels converted: %d") % stats_data.get('total_channels', 0),
                _("💾 Output file: %s") % stats_data.get('output_file', ''),
                _("📁 File size: %s") % self._format_file_size(stats_data.get('file_size', 0))
            ])
        elif conversion_type == "m3u_to_json":
            stats_message.extend([
                _("📊 Channels converted: %d") % stats_data.get('total_channels', 0),
                _("💾 Output file: %s") % stats_data.get('output_file', ''),
                _("📁 File size: %s") % self._format_file_size(stats_data.get('file_size', 0)),
                _("🎯 JSON structure: %s") % stats_data.get('json_structure', 'playlist')
            ])

        stats_message.extend([
            "",
            _("⏱️ Conversion type: %s") % conversion_type,
            _("✅ Status: Completed successfully")
        ])

        self.session.open(
            MessageBox,
            "\n".join(stats_message),
            MessageBox.TYPE_INFO,
            timeout=15
        )

    def _format_file_size(self, size_bytes):
        """Format the file size to readable format"""
        if size_bytes == 0:
            return "0 B"

        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        size = size_bytes

        while size >= 1024 and i < len(size_names) - 1:
            size /= 1024
            i += 1

        return f"{size:.2f} {size_names[i]}"

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
            _(" • Press Green to convert selection"),
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


# ==================== PLUGIN ENTRY POINT ====================
def main(session, **kwargs):
    core_converter.cleanup_old_backups(config.plugins.m3uconverter.max_backups.value)
    session.open(ConversionSelector)


def Plugins(**kwargs):
    from Plugins.Plugin import PluginDescriptor
    return [PluginDescriptor(
        name=_("Universal Converter"),
        description=_("Convert between M3U and Enigma2 bouquets"),
        where=PluginDescriptor.WHERE_PLUGINMENU,
        icon="plugin.png",
        fnc=main)
    ]


core_converter = CoreConverter()
