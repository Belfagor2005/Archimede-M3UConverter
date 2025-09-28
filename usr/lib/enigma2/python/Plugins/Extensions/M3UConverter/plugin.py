# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 2.2                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "17:30 - 20250928"                    #
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
import codecs
import json
import shutil
import unicodedata
from collections import defaultdict
from os import access, W_OK, listdir, remove, replace, chmod, system, mkdir, makedirs
from os.path import exists, isdir, isfile, join, normpath, basename, dirname, getsize
from re import compile, sub, findall, DOTALL, MULTILINE, IGNORECASE, search, escape
from threading import Lock
from time import strftime
from urllib.parse import unquote

from twisted.internet import threads

from enigma import eServiceReference, getDesktop, eTimer
try:
    from enigma import AVSwitch
except ImportError:
    from Components.AVSwitch import AVSwitch

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
import subprocess


from . import _
from .Logger_clr import get_logger

# Try to import lxml, install if not available
try:
    from lxml import etree
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    try:
        subprocess.run(["pip3", "install", "lxml"], check=True, capture_output=True, text=True)
        from lxml import etree
        LXML_AVAILABLE = True
    except Exception as e:
        print(e)
        LXML_AVAILABLE = False
        import xml.etree.ElementTree as ET

# ==================== CONSTANTS AND GLOBALS ====================
CURRENT_VERSION = '2.2'
LAST_MODIFIED_DATE = "20250928"
PLUGIN_TITLE = _("Archimede Universal Converter v.%s by Lululla") % CURRENT_VERSION
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2

ARCHIMEDE_CONVERTER_PATH = "archimede_converter"
LOG_DIR = join("/tmp", ARCHIMEDE_CONVERTER_PATH)
MAIN_LOG = join(LOG_DIR, "unified_converter.log")


# Create directory if it does not exist
try:
    makedirs(LOG_DIR, exist_ok=True)
except Exception:
    pass


logger = get_logger(
    log_path=LOG_DIR,
    plugin_name="M3U_CONVERTER",
    clear_on_start=True,
    max_size_mb=1
)

# Language to country mapping for EPG sources
LANGUAGE_TO_COUNTRY = {
    'it': 'IT',    # Italian -> Italy
    'en': 'UK',    # English -> United Kingdom
    'de': 'DE',    # German -> Germany
    'fr': 'FR',    # French -> France
    'es': 'ES',    # Spanish -> Spain
    'pt': 'PT',    # Portuguese -> Portugal
    'nl': 'NL',    # Dutch -> Netherlands
    'tr': 'TR',    # Turkish -> Turkey
    'pl': 'PL',    # Polish -> Poland
    'gr': 'GR',    # Greek -> Greece
    'cz': 'CZ',    # Czech -> Czech Republic
    'hu': 'HU',    # Hungarian -> Hungary
    'ro': 'RO',    # Romanian -> Romania
    'se': 'SE',    # Swedish -> Sweden
    'no': 'NO',    # Norwegian -> Norway
    'dk': 'DK',    # Danish -> Denmark
    'fi': 'FI',    # Finnish -> Finland
    'all': 'ALL'   # All countries
}

# ==================== UTILITY FUNCTIONS ====================


def default_movie_path():
    """Get default movie path from Enigma2 configuration."""
    result = config.usage.default_path.value
    if not result.endswith("/"):
        result += "/"
    if not isdir(result):
        return defaultRecordingLocation(config.usage.default_path.value)
    return result


def get_mounted_devices():
    """Get list of mounted and writable devices."""
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


def update_mounts_configuration():
    """Update the list of mounted devices and update config choices."""
    mounts = get_mounted_devices()
    if not mounts:
        default_path = default_movie_path()
        mounts = [(default_path, default_path)]
    config.plugins.m3uconverter.lastdir.setChoices(mounts, default=mounts[0][0])
    config.plugins.m3uconverter.lastdir.save()


def create_bouquets_backup():
    """Create a backup of bouquets.tv only."""
    from shutil import copy2
    src = "/etc/enigma2/bouquets.tv"
    dst = "/etc/enigma2/bouquets.tv.bak"
    if exists(src):
        copy2(src, dst)


def reload_enigma2_services():
    """Reload bouquets in Enigma2 with multiple fallback methods."""
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


def transliterate_text(text):
    """Convert accented characters to ASCII equivalents.

    Args:
        text (str): Text with possible accented characters

    Returns:
        str: Text with accented characters converted to ASCII
    """
    if not text:
        return ""

    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode('ascii', 'ignore').decode('ascii')


def clean_group_name(name):
    """Clean group names preserving accented characters.

    Args:
        name (str): Original group name

    Returns:
        str: Cleaned group name
    """
    if not name:
        return "Default"

    cleaned = name.strip()

    # Remove common prefixes and suffixes
    cleaned = sub(r'^\s*\|[A-Z]+\|\s*', '', cleaned)
    cleaned = sub(r'^\s*[A-Z]{2}:\s*', '', cleaned)
    cleaned = sub(r'^\s*(IT|UK|FR|DE|ES|NL|PL|GR|CZ|HU|RO|SE|NO|DK|FI)\s+', '', cleaned, flags=IGNORECASE)

    # Remove special characters but preserve accents and hyphens
    cleaned = sub(r'[^\w\s\-àèéìíòóùúÀÈÉÌÍÒÓÙÚ]', '', cleaned)
    cleaned = ' '.join(cleaned.split())

    # Limit length
    if len(cleaned) > 40:
        cleaned = cleaned[:40]

    return cleaned or "Default"


# ==================== CONFIG INITIALIZATION ====================
config.plugins.m3uconverter = ConfigSubsection()
default_dir = config.movielist.last_videodir.value if isdir(config.movielist.last_videodir.value) else default_movie_path()
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
config.plugins.m3uconverter.platform_priority = ConfigSelection(
    default="terrestrial,cable,satellite,iptv",
    choices=[
        ("satellite,terrestrial,cable,iptv", _("Satellite > Terrestrial > Cable > IPTV")),
        ("terrestrial,cable,satellite,iptv", _("Terrestrial > Cable > Satellite > IPTV")),
        ("cable,terrestrial,satellite,iptv", _("Cable > Terrestrial > Satellite > IPTV")),
    ]
)
config.plugins.m3uconverter.epg_generation_mode = ConfigSelection(
    default="epgshare",
    choices=[("epgshare", _("EPGShare Mode")), ("standard", _("Standard Mode"))]
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
    "all": "All Countries - IPTV",
}, default="all")


update_mounts_configuration()


# ==================== CORE CLASSES ====================
class AspectManager:
    """Manage aspect ratio settings for video playback."""
    def __init__(self):
        self.init_aspect = self.get_current_aspect()
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Initial aspect ratio: {self.init_aspect}")

    def getAspectRatioSetting(self):
        """
        Map Enigma2 config.av.aspectratio string values to integer codes.
        If no mapping exists, fall back to the raw config value.
        """
        aspect_map = {
            "4_3_letterbox": 0,
            "4_3_panscan": 1,
            "16_9": 2,
            "16_9_always": 3,
            "16_10_letterbox": 4,
            "16_10_panscan": 5,
            "16_9_letterbox": 6,
        }

        val = config.av.aspectratio.value
        return aspect_map.get(val, val)

    def set_aspect_for_video(self, aspect=None):
        """Temporarily set an aspect ratio for video playback."""
        try:
            if aspect is None:
                aspect = 2
            print("[INFO] Imposto aspect ratio a:", aspect)
            AVSwitch().setAspectRatio(aspect)
        except Exception as e:
            logger.error(f"Failed to set aspect ratio: {str(e)}")

    def get_current_aspect(self):
        """Get the current aspect ratio setting.

        Returns:
            int: Current aspect ratio setting
        """
        try:
            return self.getAspectRatioSetting()
        except Exception as e:
            logger.error(f"Failed to get aspect ratio: {str(e)}")
            return 0

    def restore_aspect(self):
        """Restores the original aspect ratio when the plugin exits."""
        try:
            logger.info(f"Restoring aspect ratio to: {self.init_aspect}")
            AVSwitch().setAspectRatio(self.init_aspect)
        except Exception as e:
            logger.error(f"Failed to restore aspect ratio: {str(e)}")


# ==================== GLOBAL INSTANCES ====================
aspect_manager = AspectManager()
screen_dimensions = getDesktop(0).size()
SCREEN_WIDTH = screen_dimensions.width()


class UnifiedChannelMapping:
    """Unified channel mapping structure to replace multiple redundant maps."""
    def __init__(self):
        """Initialize unified channel mapping with empty structures."""
        self.rytec = {
            'basic': {},                    # Base Rytec mapping (id -> sref)
            'clean': {},                    # Clean names mapping (clean_name -> sref)
            'extended': defaultdict(list)   # Extended info with variants (id -> [variants])
        }
        self.dvb = defaultdict(list)     # DVB channels from lamedb/bouquets (name -> [services])
        self.optimized = {}              # Optimized for matching (name -> best_service)
        self.reverse_mapping = {}        # Reverse mapping for Sky channels (channel_id -> satellite)
        self.auto_discovered = {}        # Auto-discovered references (channel_id -> sref)
        self._clean_name_cache = {}      # Cache for cleaned names
        self._clean_cache_max_size = 10000

    def clear(self):
        """Clear all mappings."""
        self.rytec['basic'].clear()
        self.rytec['clean'].clear()
        self.rytec['extended'].clear()
        self.dvb.clear()
        self.optimized.clear()
        self.reverse_mapping.clear()
        self.auto_discovered.clear()
        self._clean_name_cache.clear()


class EPGServiceMapper:
    """Service mapper for EPG data matching and conversion."""
    def __init__(self, prefer_satellite=True):
        """Initialize EPG service mapper.

        Args:
            prefer_satellite (bool): Whether to prefer satellite services
        """
        self._match_cache = {}
        self._match_cache_hits = 0
        self._match_cache_misses = 0
        self._incompatible_matches = 0
        self._cache_max_size = 5000
        self._cache_cleanup_threshold = 100

        self.epg_cache = {}
        self.epg_cache_hits = 0
        self.epg_cache_misses = 0

        # Cache separata per nomi puliti (molto utilizzata)
        self._clean_cache_max_size = 10000
        self._clean_name_cache = {}

        self.channel_cache = ChannelCache(max_size=3000)

        self._rytec_lock = Lock()
        self.epg_share_loaded = False
        self.enigma_config = self._load_enigma2_configuration()
        self.country_code = self._get_system_country_code()

        self.prefer_satellite = prefer_satellite
        self.mapping = UnifiedChannelMapping()

        # Pre-compiled regex patterns
        self._clean_pattern = compile(r'[^\w\s\-àèéìíòóùúÀÈÉÌÍÒÓÙÚ]', IGNORECASE)
        self._quality_pattern = compile(
            r'\b(4k|uhd|fhd|hd|sd|hq|uhq|sdq|hevc|h265|h264|h\.265|h\.264|full hd|ultra hd|high definition|standard definition)\b',
            IGNORECASE
        )

        # memory optimization timer
        self.optimize_memory_timer = eTimer()
        self.optimize_memory_timer.callback.append(self.optimize_memory_usage)
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("EPGServiceMapper initialized with unified mapping")

    def _get_from_cache(self, cache_key):
        """Get result from cache and track hits/misses"""
        if cache_key in self._match_cache:
            self._match_cache_hits += 1
            return self._match_cache[cache_key]
        else:
            self._match_cache_misses += 1
            return None

    def _get_from_cache_optimized(self, cache_key):
        """Optimized version of cache lookup"""
        short_key = hash(cache_key) % 10000

        if short_key in self._match_cache:
            self._match_cache_hits += 1
            return self._match_cache[short_key]
        else:
            self._match_cache_misses += 1
            return None

    def _add_to_cache_optimized(self, cache_key, result, match_type):
        """Optimized cache addition with automatic cleaning"""
        if not result:
            return

        short_key = hash(cache_key) % 10000

        if len(self._match_cache) >= self._cache_max_size:
            items_to_remove = int(self._cache_max_size * 0.2)
            for key in list(self._match_cache.keys())[:items_to_remove]:
                del self._match_cache[key]

        if self.is_service_compatible(result):
            self._match_cache[short_key] = (result, match_type)
        else:
            self._incompatible_matches += 1

    def get_cache_statistics(self):
        """Return detailed cache statistics"""
        if config.plugins.m3uconverter.enable_debug.value:
            print(f"[DEBUG] Cache hits: {self._match_cache_hits}")
            print(f"[DEBUG] Cache misses: {self._match_cache_misses}")
            print(f"[DEBUG] Cache size: {len(self._match_cache)}")
            print(f"[DEBUG] Incompatible matches: {self._incompatible_matches}")
        total_requests = self._match_cache_hits + self._match_cache_misses

        hit_rate = (self._match_cache_hits / total_requests * 100) if total_requests > 0 else 0

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

        # Fix: Add EPG cache statistics with safe access
        epg_cache_size = len(self.epg_cache) if hasattr(self, 'epg_cache') else 0
        epg_total_requests = (self.epg_cache_hits + self.epg_cache_misses) if hasattr(self, 'epg_cache_hits') and hasattr(self, 'epg_cache_misses') else 0
        epg_hit_rate = (self.epg_cache_hits / epg_total_requests * 100) if epg_total_requests > 0 else 0

        return {
            'hits': self._match_cache_hits,
            'misses': self._match_cache_misses,
            'total_requests': total_requests,
            'hit_rate': f"{hit_rate:.1f}%",
            'cache_size': len(self._match_cache),
            'cache_analysis': cache_analysis,
            'incompatible_matches': self._incompatible_matches,
            'loaded_dvb_channels': len(self.mapping.dvb),
            'rytec_channels': len(self.mapping.rytec['extended']),
            # EPG cache stats
            'epg_cache_size': epg_cache_size,
            'epg_hits': self.epg_cache_hits if hasattr(self, 'epg_cache_hits') else 0,
            'epg_misses': self.epg_cache_misses if hasattr(self, 'epg_cache_misses') else 0,
            'epg_total_requests': epg_total_requests,
            'epg_hit_rate': f"{epg_hit_rate:.1f}%",
            'total_hits': self._match_cache_hits + (self.epg_cache_hits if hasattr(self, 'epg_cache_hits') else 0),
            'overall_hit_rate': f"{((self._match_cache_hits + (self.epg_cache_hits if hasattr(self, 'epg_cache_hits') else 0)) / (total_requests + epg_total_requests) * 100) if (total_requests + epg_total_requests) > 0 else 0:.1f}%"
        }

    def reset_caches(self):
        """Reset all caches before a new conversion."""
        # Match Cache
        self._match_cache.clear()
        self._match_cache_hits = 0
        self._match_cache_misses = 0

        # EPG Cache - Fix: Initialize if not exists
        if not hasattr(self, 'epg_cache'):
            self.epg_cache = {}
            self.epg_cache_hits = 0
            self.epg_cache_misses = 0
        else:
            self.epg_cache.clear()
            self.epg_cache_hits = 0
            self.epg_cache_misses = 0

        # Altre statistiche
        self._incompatible_matches = 0

        # Channel Cache (se presente)
        if hasattr(self, 'channel_cache'):
            self.channel_cache._cache.clear()
            self.channel_cache._hits = 0
            self.channel_cache._misses = 0

        logger.debug("All caches reset for new conversion")

    def _load_enigma2_configuration(self, settings_path="/etc/enigma2/settings"):
        """Load Enigma2 configuration to determine configured satellites.

        Args:
            settings_path (str): Path to Enigma2 settings file

        Returns:
            dict: Configuration data including satellites, terrestrial, cable info
        """
        config_data = {'satellites': set(), 'terrestrial': False, 'cable': False}

        if not fileExists(settings_path):
            if config.plugins.m3uconverter.enable_debug.value:
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
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Enigma2 configuration loaded: %s", config_data)
            return config_data
        except Exception as e:
            logger.error("Error reading Enigma2 settings: %s", str(e))
            return config_data

    def load_channel_mapping(self, mapping_path="/usr/lib/enigma2/python/Plugins/Extensions/M3UConverter/channel_mapping.conf"):
        """Load channel ID mapping from external file with improved parsing.

        Args:
            mapping_path (str): Path to channel mapping file

        Returns:
            bool: True if loading was successful, False otherwise
        """
        self.channel_mapping = {}
        self.reverse_channel_mapping = {}  # Reverse map: channel ID -> satellite

        if not fileExists(mapping_path):
            if config.plugins.m3uconverter.enable_debug.value:
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

    def get_epg_url_for_language(self, language_code):
        """Return the correct EPG URL based on language selection.

        Args:
            language_code (str): Two-letter language code

        Returns:
            str: EPG URL for the specified language
        """
        country_code = LANGUAGE_TO_COUNTRY.get(language_code, 'ALL')

        epg_urls = {
            'IT': 'https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz',
            'UK': 'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz',
            'DE': 'https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz',
            'FR': 'https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz',
            'ES': 'https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz',
            'NL': 'https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz',
            'PL': 'https://epgshare01.online/epgshare01/epg_ripper_PL1.xml.gz',
            'GR': 'https://epgshare01.online/epgshare01/epg_ripper_GR1.xml.gz',
            'CZ': 'https://epgshare01.online/epgshare01/epg_ripper_CZ1.xml.gz',
            'HU': 'https://epgshare01.online/epgshare01/epg_ripper_HU1.xml.gz',
            'RO': 'https://epgshare01.online/epgshare01/epg_ripper_RO1.xml.gz',
            'SE': 'https://epgshare01.online/epgshare01/epg_ripper_SE1.xml.gz',
            'NO': 'https://epgshare01.online/epgshare01/epg_ripper_NO1.xml.gz',
            'DK': 'https://epgshare01.online/epgshare01/epg_ripper_DK1.xml.gz',
            'FI': 'https://epgshare01.online/epgshare01/epg_ripper_FI1.xml.gz',
            'ALL': 'https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz'
        }

        return epg_urls.get(country_code, epg_urls['ALL'])

    def is_service_compatible(self, service_ref):
        """Check if service is compatible with current configuration.

        Args:
            service_ref (str): Service reference to check

        Returns:
            bool: True if service is compatible, False otherwise
        """
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
            onid_to_position = {
                0x13E: 130,     # 13.0°E Hotbird
                0x110: 130,     # 13.0°E Eutelsat
                0x1:   192,     # 19.2°E Astra
                0x2:   282,     # 28.2°E Astra
                0x11:  235,     # 23.5°E Astra
                0x10:   90,     # 9.0°E Eutelsat 9B
                0xFFFF: 0,      # Cable services
                0xEEEE: 0,      # Terrestrial services
            }
            if onid in onid_to_position:
                position = onid_to_position[onid]
                if position == 0:  # Cable/Terrestrial
                    if onid == 0xFFFF and self.enigma_config['cable']:
                        return True
                    elif onid == 0xEEEE and self.enigma_config['terrestrial']:
                        return True
                else:  # Satellite
                    return position in self.enigma_config['satellites']

            # Fallback for other service types
            if service_type == '16' and self.enigma_config['terrestrial']:
                return True
            elif service_type == '10' and self.enigma_config['cable']:
                return True

        except (ValueError, IndexError):
            pass
        return False

    def classify_service_type(self, service_ref):
        """Classify service type based on service reference.

        Args:
            service_ref (str): Service reference to classify

        Returns:
            str: Service type (satellite, dvb-t, dvb-c, iptv, unknown)
        """
        if not service_ref:
            return "unknown"

        parts = service_ref.split(':')
        if len(parts) < 11:
            return "unknown"

        service_type = parts[2]
        namespace = parts[6] if len(parts) > 6 else ""

        # Classify by namespace FIRST
        if namespace == "820000":
            return "satellite"
        elif namespace == "EEEE":
            return "dvb-t"
        elif namespace == "FFFF":
            return "dvb-c"
        elif service_type == "16":
            return "dvb-t"
        elif service_type == "10":
            return "dvb-c"
        elif service_type == "1":
            return "satellite"
        elif service_ref.startswith("4097:"):
            return "iptv"

        return "unknown"

    def _get_system_country_code(self):
        """Get country code from plugin configuration with fallbacks.

        Returns:
            str: Two-letter country code
        """
        try:
            if (hasattr(config.plugins, 'm3uconverter') and
                    hasattr(config.plugins.m3uconverter, 'language')):
                lang_setting = config.plugins.m3uconverter.language.value
                if lang_setting != 'all':
                    return lang_setting.lower()

            settings_path = "/etc/enigma2/settings"
            if fileExists(settings_path):
                with open(settings_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('config.misc.country='):
                            country_code = line.split('=', 1)[1].strip()
                            return country_code.lower()

            import time
            timezone = time.tzname[0] if time.tzname else ''
            if 'CET' in timezone or 'CEST' in timezone:
                return 'it'

            return 'eu'

        except Exception as e:
            logger.error(f"Error getting country code: {str(e)}")
            return 'eu'

    def _generate_rytec_style_id(self, channel_name, service_ref):
        """Generate Rytec-style ID for EPG XMLTV compatibility.

        Args:
            channel_name (str): Channel name
            service_ref (str): Service reference

        Returns:
            str: Rytec-style channel ID
        """
        if not channel_name:
            return "unknown"

        clean_name = self.clean_channel_name(channel_name)
        clean_name = clean_name.replace(' ', '.').lower()
        clean_name = sub(r'[^a-z0-9.]', '', clean_name)

        country_suffix = ""
        country_code = ""
        if service_ref:
            country_code = self._get_country_from_service_ref(service_ref)

        if country_code:
            country_suffix = f".{country_code}"
        else:
            country_suffix = ""

        if len(clean_name) > 30:
            clean_name = clean_name[:30]

        return f"{clean_name}{country_suffix}"

    def _get_country_from_service_ref(self, service_ref):
        """Try to infer country from service reference"""
        if not service_ref or ':' not in service_ref:
            return None

        parts = service_ref.split(':')
        if len(parts) < 6:
            return None

        onid = parts[5]

        onid_to_country = {
            '13E': '', '110': '',      # Hotbird - Europe
            '1': '', '2': '',          # Astra - Europe
            '11': '', '10': '',        # Astra - Europe
            'FFFF': '', 'EEEE': '',    # Cable/Terrestrial - local
            '217C': 'it',              # DVB-T Italy
            '22F1': 'nl',              # DVB-T Netherlands
            '22C1': 'be',              # DVB-T Belgium
            '20FA': 'de',              # DVB-T Germany
            '22F2': 'uk',              # DVB-T UK
        }

        return onid_to_country.get(onid, None)

    def clean_channel_name(self, name, preserve_variants=False):
        """Clean channel name with option to preserve variants.

        Args:
            name (str): Original channel name
            preserve_variants (bool): Whether to preserve quality variants

        Returns:
            str: Cleaned channel name
        """
        if not name:
            return ""

        cache_key = f"{name}_{preserve_variants}"
        if cache_key in self.mapping._clean_name_cache:
            return self.mapping._clean_name_cache[cache_key]

        try:
            cleaned = name.lower().strip()

            # Remove prefix
            cleaned = sub(r'^\s*\|[A-Z]{2,}\|\s*', '', cleaned)
            cleaned = sub(r'^\s*\|[^|]+\|\s*', '', cleaned)

            # If preserve_variants is False, remove quality variants
            if not preserve_variants:
                cleaned = self._quality_pattern.sub('', cleaned)

            # General cleaning
            for char in '()[]{}|/\\_—–-+':
                cleaned = cleaned.replace(char, ' ')
            cleaned = sub(r'[^a-z0-9\s]', '', cleaned)
            cleaned = ' '.join(cleaned.split()).strip()
            # Cache the result
            self.mapping._clean_name_cache[cache_key] = cleaned
            return cleaned

        except Exception as e:
            logger.error(f"Error cleaning channel name '{name}': {str(e)}")
            return name.lower() if name else ""

    def optimize_matching(self):
        """Optimize channel map structures for faster matching."""
        self.mapping.optimized.clear()
        for name, services in self.mapping.dvb.items():
            if not services:
                continue

            bouquet_services = [s for s in services if s["source"] == "bouquet"]

            satellite_services = [s for s in services if s["type"] == "satellite" and s["source"] == "lamedb"]

            other_services = [s for s in services if s not in bouquet_services + satellite_services]

            if bouquet_services:
                main_service = bouquet_services[0]
            elif satellite_services:
                main_service = satellite_services[0]
            elif other_services:
                main_service = other_services[0]
            else:
                continue

            clean_name = self.clean_channel_name(name)
            self.mapping.optimized[name] = main_service
            if clean_name not in self.mapping.optimized:
                self.mapping.optimized[clean_name] = main_service

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Optimized channel map built: {len(self.mapping.optimized)} entries")

    def parse_lamedb(self, lamedb_path="/etc/enigma2/lamedb"):
        """Parse both lamedb and lamedb5 using unified mapping.

        Args:
            lamedb_path (str): Path to lamedb file

        Returns:
            bool: True if parsing was successful, False otherwise
        """
        paths_to_try = [
            "/etc/enigma2/lamedb5",
            "/etc/enigma2/lamedb"
        ]

        for lamedb_path in paths_to_try:
            if not fileExists(lamedb_path):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Lamedb file not found: {lamedb_path}")
                continue

            try:
                with open(lamedb_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                # Check if it's a transponder file
                if content.strip().startswith("p:") and "services" not in content:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Lamedb file {lamedb_path} appears to be a transponder list")
                    continue

                # Identify the file format
                if content.startswith("eDVB services /5/"):
                    self._parse_lamedb5_format(content)
                else:
                    self._parse_legacy_lamedb_format(content)

                # Filter incompatible services
                for name in list(self.mapping.dvb.keys()):
                    compatible_services = self.filter_compatible_services(self.mapping.dvb[name])
                    if compatible_services:
                        self.mapping.dvb[name] = compatible_services
                    else:
                        del self.mapping.dvb[name]

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Parsed {0} unique compatible DVB channel names from {1}".format(len(self.mapping.dvb), lamedb_path))
                    # Debug found namespaces
                    namespaces = defaultdict(int)
                    for services in self.mapping.dvb.values():
                        for service in services:
                            if service["source"] == "lamedb5":
                                namespaces[service.get("namespace", "unknown")] += 1

                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("Found namespaces: {0}".format(dict(namespaces)))
                return True

            except Exception as e:
                logger.error("Error parsing {0}: {1}".format(lamedb_path, str(e)))

        if config.plugins.m3uconverter.enable_debug.value:
            logger.error("Could not find or parse any lamedb file")
        return False

    def _parse_lamedb5_format(self, content):
        """Parse lamedb5 file format.

        Args:
            content (str): File content to parse
        """
        lines = content.split("\n")
        for line in lines:
            if line.startswith("s:"):
                parts = line.split(",", 2)
                if len(parts) >= 2:
                    sref_parts = parts[0][2:].split(":")
                    if len(sref_parts) >= 6:
                        service_id = sref_parts[0]
                        namespace = sref_parts[1]
                        ts_id = sref_parts[2]
                        on_id = sref_parts[3]
                        service_type = sref_parts[4]

                        # Handle namespace correctly
                        if namespace == "00820000":
                            namespace = "820000"
                            service_type = "1"
                        elif namespace.startswith("eeee"):
                            namespace = "EEEE"
                            service_type = "16"
                        elif namespace.startswith("ffff"):
                            namespace = "FFFF"
                            service_type = "10"
                        else:
                            if len(namespace) > 4:
                                namespace = namespace[:4]

                        channel_name = parts[1].strip('"')
                        service_ref = f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

                        clean_name = self.clean_channel_name(channel_name)

                        self.mapping.dvb[clean_name].append({
                            "sref": service_ref,
                            "type": self.classify_service_type(service_ref),
                            "source": "lamedb5",
                            "service_id": service_id,
                            "ts_id": ts_id,
                            "on_id": on_id,
                            "namespace": namespace
                        })

    def _parse_legacy_lamedb_format(self, content):
        """Parse traditional lamedb file format.
        Args:
            content (str): File content to parse
        """
        lines = content.split("\n")
        for line in lines:
            if line.startswith("s:"):
                parts = line.split(",")
                if len(parts) >= 2:
                    sref_part = parts[0]
                    channel_name = parts[1].strip('"')
                    sref_parts = sref_part.split(":")
                    if len(sref_parts) >= 6:
                        service_id = sref_parts[1]
                        on_id = sref_parts[2]
                        if len(on_id) > 4:
                            on_id = on_id[:4]
                        ts_id = sref_parts[3]
                        service_type = sref_parts[4]

                        service_ref = "1:0:{0}:{1}:{2}:{3}:820000:0:0:0:".format(
                            service_type, service_id, ts_id, on_id
                        )
                        clean_name = self.clean_channel_name(channel_name)
                        self.mapping.dvb[clean_name].append({
                            "sref": service_ref,
                            "type": self.classify_service_type(service_ref),
                            "source": "lamedb",
                            "service_id": service_id,
                            "ts_id": ts_id,
                            "on_id": on_id
                        })

    def parse_rytec_channels(self, rytec_path=None):
        """Parse rytec.channels.xml using unified mapping.

        Args:
            rytec_path (str): Path to rytec channels file
        """
        if rytec_path and fileExists(rytec_path):
            rytec_path = rytec_path
        else:
            rytec_path = "/etc/epgimport/rytec.channels.xml"

        if not fileExists(rytec_path):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.warning("Rytec file not found: %s", rytec_path)
            return

        try:
            with open(rytec_path, "r", encoding="utf-8") as f:
                content = f.read()

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Rytec file found, size: %d bytes", len(content))

            pattern = r'(<!--\s*([^>]+)\s*-->)?\s*<channel id="([^"]+)">([^<]+)</channel>\s*(?:<!--\s*([^>]+)\s*-->)?'
            matches = findall(pattern, content)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Found %d channel entries in rytec file", len(matches))

            with self._rytec_lock:
                for match in matches:
                    comment_before, source_info, channel_id, service_ref, comment_after = match
                    comment = comment_before or comment_after or ""

                    # Extract the real channel name
                    channel_name = self._extract_real_channel_name(comment)

                    normalized_ref = self.normalize_service_reference(service_ref, for_epg=True)

                    if self.is_service_compatible(normalized_ref):
                        # Extended database with all info
                        self.mapping.rytec['extended'][channel_id].append({
                            'sref': normalized_ref,
                            'comment': comment.strip(),
                            'channel_name': channel_name,
                            'source_type': self._get_source_type(comment),
                            'sat_position': self._extract_satellite_position(comment)
                        })

                        # KEEP COMPATIBILITY
                        if channel_id not in self.mapping.rytec['basic']:
                            self.mapping.rytec['basic'][channel_id] = normalized_ref

                        clean_base_id = self.clean_channel_name(channel_id.split('.')[0])
                        self.mapping.rytec['clean'][clean_base_id] = normalized_ref

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Parsed %d Rytec channels with extended info", len(self.mapping.rytec['extended']))

        except Exception as e:
            logger.error("Error parsing rytec.channels.xml: %s", str(e))

    def parse_existing_bouquets(self, bouquet_dir="/etc/enigma2"):
        """Parse all existing bouquets for current service references.

        Args:
            bouquet_dir (str): Directory containing bouquet files
        """
        bouquet_files = []

        # First read the main bouquets.tv file
        bouquets_file = join(bouquet_dir, "bouquets.tv")
        if fileExists(bouquets_file):
            with open(bouquets_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if 'FROM BOUQUET "' in line:
                        match = search(r'FROM BOUQUET "([^"]+)"', line)
                        if match:
                            bouquet_files.append(join(bouquet_dir, match.group(1)))

        # Add all userbouquet files
        for filename in listdir(bouquet_dir):
            if filename.startswith('userbouquet.') and filename.endswith('.tv'):
                bouquet_files.append(join(bouquet_dir, filename))

        # Parse each bouquet
        for bouquet_file in bouquet_files:
            if not fileExists(bouquet_file):
                continue

            try:
                with open(bouquet_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                    service_pattern = r'#SERVICE (\d+:\d+:\d+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:)'
                    matches = findall(service_pattern, content)

                    for service_ref in matches:
                        if not service_ref.startswith('4097:'):  # Ignore IPTV services
                            desc_pattern = r'#DESCRIPTION (.+)\n'
                            desc_match = search(desc_pattern, content[content.find(service_ref):])
                            channel_name = desc_match.group(1).strip() if desc_match else "Unknown"

                            clean_name = self.clean_channel_name(channel_name)

                            self.mapping.dvb[clean_name].append({
                                "sref": service_ref,
                                "type": self.classify_service_type(service_ref),
                                "source": "bouquet",
                                "service_id": service_ref.split(':')[3],
                                "ts_id": service_ref.split(':')[4],
                                "on_id": service_ref.split(':')[5]
                            })

            except Exception as e:
                logger.error(f"Error parsing bouquet {bouquet_file}: {str(e)}")

    def _extract_real_channel_name(self, comment):
        """Extract the real channel name from the comment.

        Args:
            comment (str): Comment containing channel name

        Returns:
            str: Extracted channel name
        """
        if not comment:
            return ""

        parts = comment.split('-->')
        if len(parts) > 1:
            return parts[-1].strip()

        return comment.strip()

    def _extract_satellite_position(self, comment):
        """Extract the satellite position from the comment.

        Args:
            comment (str): Comment containing satellite info

        Returns:
            str: Satellite position or None
        """
        position_match = search(r'(\d+\.\d+[EW])', comment)
        return position_match.group(1) if position_match else None

    def _get_source_type(self, comment):
        """Determine source type with greater precision.

        Args:
            comment (str): Comment containing source info

        Returns:
            str: Source type
        """
        if not comment:
            return 'unknown'

        comment_lower = comment.lower()

        satellite_positions = {
            '13.0e': 'hotbird', '13e': 'hotbird', '13°e': 'hotbird',
            '19.2e': 'astra19', '19e': 'astra19', '19.2': 'astra19',
            '28.2e': 'astra28', '28e': 'astra28', '28.2': 'astra28',
            '23.5e': 'astra23', '23e': 'astra23',
            '5.0w': 'amazonas', '5w': 'amazonas',
            '0.8w': 'thor', '0.8w': 'thor',
            '4.8e': 'sirius', '4.8': 'sirius',
            '7.0w': 'nilesat', '7w': 'nilesat',
            '9.0e': 'eutelsat9', '9e': 'eutelsat9'
        }

        for pos_key, pos_name in satellite_positions.items():
            if pos_key in comment_lower:
                return f'satellite_{pos_name}'

        if any(x in comment_lower for x in ['iptv', 'http', 'https', 'stream']):
            return 'iptv'
        elif any(x in comment_lower for x in ['terrestre', 'dvb-t', 'tnt', 'antenna']):
            return 'terrestrial'
        elif any(x in comment_lower for x in ['cable', 'dvbc', 'via cavo']):
            return 'cable'
        elif any(x in comment_lower for x in ['misc', 'varie', 'other']):
            return 'misc'

        return 'unknown'

    def filter_compatible_services(self, services):
        """Filter services compatible with configuration.

        Args:
            services (list): List of service dictionaries

        Returns:
            list: Filtered list of compatible services
        """
        compatible_services = []

        for service in services:
            service_ref = service['sref']
            service_type = service.get('type', 'unknown')
            comment = service.get('comment', '')

            if service_type == 'iptv' or service_ref.startswith('4097:'):
                compatible_services.append(service)
                continue

            if service_type in ['terrestrial', 'cable']:
                compatible_services.append(service)
                continue

            if service_type == 'satellite':
                if self.is_satellite_compatible(comment):
                    compatible_services.append(service)
                else:
                    logger.debug(f"Filtered out incompatible satellite service: {comment}")
            else:
                compatible_services.append(service)

        return compatible_services

    def is_satellite_compatible(self, comment):
        """Check if satellite service is compatible with current configuration.

        Args:
            comment (str): Comment containing satellite info

        Returns:
            bool: True if compatible, False otherwise
        """
        if not comment:
            return True

        comment_lower = comment.lower()

        # Main satellites that are usually configured
        main_satellites = [
            '13.0e', '13e', 'hotbird',
            '19.2e', '19e', 'astra19',
            '28.2e', '28e', 'astra28',
            '23.5e', '23e', 'astra23'
        ]

        for satellite in main_satellites:
            if satellite in comment_lower:
                return True

        return False

    def _find_phonetic_match(self, clean_name):
        """Search for phonetic matches using Metaphone algorithm.

        Args:
            clean_name (str): Cleaned channel name

        Returns:
            str: Service reference if match found, None otherwise
        """
        try:
            try:
                from fuzzy import metaphone
                USE_METAPHONE = True
            except ImportError:
                USE_METAPHONE = False
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("fuzzy module not available, using similarity matching")

            if USE_METAPHONE:
                clean_metaphone = metaphone(clean_name)

                for rytec_id, variants in self.mapping.rytec['extended'].items():
                    for variant in variants:
                        if variant.get('channel_name'):
                            variant_clean = self.clean_channel_name(variant['channel_name'])
                            variant_metaphone = metaphone(variant_clean)

                            if clean_metaphone == variant_metaphone:
                                if self.is_service_compatible(variant['sref']):
                                    return variant['sref']

            return self._find_similarity_match(clean_name)

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Phonetic matching error: {str(e)}")
            return self._find_similarity_match(clean_name)

    def _find_similarity_match(self, clean_name):
        """Find matches using similarity algorithms for international channels.

        Args:
            clean_name (str): Cleaned channel name

        Returns:
            str: Service reference if match found, None otherwise
        """
        best_match = None
        best_score = 0

        # Search in Rytec database
        for rytec_id, variants in self.mapping.rytec['extended'].items():
            for variant in variants:
                if variant['channel_name']:
                    variant_clean = self.clean_channel_name(variant['channel_name'])
                    similarity = self._calculate_similarity(clean_name, variant_clean)

                    if similarity > 0.85 and similarity > best_score:  # Higher threshold for international matching
                        if self.is_service_compatible(variant['sref']):
                            best_score = similarity
                            best_match = variant['sref']

        # Search in DVB database
        for db_name, services in self.mapping.dvb.items():
            similarity = self._calculate_similarity(clean_name, db_name)
            if similarity > 0.85 and similarity > best_score:
                for service in services:
                    if self.is_service_compatible(service['sref']):
                        best_score = similarity
                        best_match = service['sref']
                        break

        return best_match

    def find_best_service_match(self, clean_name, tvg_id=None, original_name=""):
        """Complete Rytec matching overhaul with proper EPG handling."""
        if not clean_name or not isinstance(clean_name, str):
            return None, 'invalid_input'

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔍 EPG SEARCH: '{original_name}' -> clean: '{clean_name}', tvg_id: '{tvg_id}'")

        cache_key = f"{clean_name}_{tvg_id}" if tvg_id else clean_name
        
        # Check cache first
        cached_result = self._get_from_cache_optimized(cache_key)
        if cached_result:
            result, match_type = cached_result
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"📦 Cached match: {clean_name} -> {match_type}")
            return result, f'cached_{match_type}'

        result = None
        match_type = 'no_match'

        # DEBUG: Show what we have in Rytec database
        if config.plugins.m3uconverter.enable_debug.value and len(self.mapping.rytec['extended']) > 0:
            logger.info(f"📊 Rytec DB size: {len(self.mapping.rytec['extended'])} channels")
            # Show first 5 Rytec channels for debugging
            for i, (rytec_id, variants) in enumerate(list(self.mapping.rytec['extended'].items())[:5]):
                logger.info(f"   Rytec {i}: {rytec_id} -> {variants[0].get('channel_name', 'Unknown')}")

        # STRATEGY 1: Exact tvg_id match in Rytec database
        if tvg_id:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🎯 Searching exact tvg_id: {tvg_id}")
            
            # Try exact match first
            if tvg_id in self.mapping.rytec['basic']:
                service_ref = self.mapping.rytec['basic'][tvg_id]
                if service_ref and self.is_service_compatible(service_ref):
                    result = service_ref
                    match_type = 'rytec_exact'
                    logger.info(f"✅ Exact Rytec match: {tvg_id} -> {service_ref}")
            
            # Try extended Rytec mapping
            if not result and tvg_id in self.mapping.rytec['extended']:
                for variant in self.mapping.rytec['extended'][tvg_id]:
                    if variant.get('sref') and self.is_service_compatible(variant['sref']):
                        result = variant['sref']
                        match_type = 'rytec_extended'
                        logger.info(f"✅ Extended Rytec match: {tvg_id} -> {result}")
                        break

        # STRATEGY 2: Search by clean name in Rytec database
        if not result:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔍 Searching by name: '{clean_name}'")
            
            best_rytec_score = 0
            best_rytec_match = None
            
            for rytec_id, variants in self.mapping.rytec['extended'].items():
                for variant in variants:
                    variant_name = variant.get('channel_name', '')
                    if not variant_name:
                        continue
                        
                    variant_clean = self.clean_channel_name(variant_name, preserve_variants=False)
                    
                    # Calculate similarity
                    similarity = self._calculate_similarity(clean_name, variant_clean)
                    
                    if similarity > 0.7 and similarity > best_rytec_score:
                        if variant.get('sref') and self.is_service_compatible(variant['sref']):
                            best_rytec_score = similarity
                            best_rytec_match = variant['sref']
                            match_type = f'rytec_similarity_{int(similarity * 100)}'
                            
                            if config.plugins.m3uconverter.enable_debug.value and similarity > 0.8:
                                logger.info(f"✅ Rytec name match: '{clean_name}' -> '{variant_name}' (score: {similarity:.2f})")
            
            if best_rytec_match:
                result = best_rytec_match

        # STRATEGY 3: Search in DVB database as fallback
        if not result:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔄 Falling back to DVB search for: '{clean_name}'")
            
            best_dvb_score = 0
            best_dvb_match = None
            
            for db_name, services in self.mapping.dvb.items():
                similarity = self._calculate_similarity(clean_name, db_name)
                
                if similarity > 0.6 and similarity > best_dvb_score:
                    for service in services:
                        if self.is_service_compatible(service['sref']):
                            best_dvb_score = similarity
                            best_dvb_match = service['sref']
                            match_type = f'dvb_fallback_{int(similarity * 100)}'
                            break
            
            if best_dvb_match:
                result = best_dvb_match
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔄 DVB fallback match: '{clean_name}' -> (score: {best_dvb_score:.2f})")

        # STRATEGY 4: Final fallback - IPTV service reference
        if not result:
            result = self.generate_service_reference("iptv_stream")
            match_type = 'iptv_fallback'
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"❌ No match found, using IPTV fallback for: '{clean_name}'")

        # Cache the result
        if result:
            self._add_to_cache_optimized(cache_key, result, match_type)
            if config.plugins.m3uconverter.enable_debug.value and 'rytec' in match_type:
                logger.info(f"💾 Cached Rytec match: {clean_name} -> {match_type}")
        
        return result, match_type

    def find_best_service_matchOOOOOOOOOOOOOOOOOOOOOOOOO(self, clean_name, tvg_id=None, original_name=""):
        """Advanced matching with fallback to DVB services."""
        if not clean_name or not isinstance(clean_name, str):
            return None, 'invalid_input'

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"SEARCH: '{original_name}' -> cleaned: '{clean_name}', tvg_id: '{tvg_id}'")

        if len(clean_name) > 100:
            clean_name = clean_name[:100]

        """
        # 0. TEST: Try phonetic matching first with validation
        # try:
            # phonetic_match = self._find_phonetic_match(clean_name)
            # if phonetic_match:
                # logger.info(f"PHONETIC MATCH FOUND: {clean_name} -> {phonetic_match}")
                # return phonetic_match, 'phonetic_match'
        # except Exception as e:
            # if config.plugins.m3uconverter.enable_debug.value:
                # logger.error(f"Phonetic matching failed: {str(e)}")
        """

        def matching_operation():
            cache_key = f"{clean_name[:50]}_{tvg_id[:20] if tvg_id else 'no_id'}"

            # === CACHE LOOKUP OPTIMIZED ===
            cached_result = self._get_from_cache_optimized(cache_key)
            if cached_result:
                result, match_type = cached_result
                return result, f'cached_{match_type}'

            # 🚨 INITIALIZE result and match_type
            result = None
            match_type = 'no_match'

            # 0. TEST: Try phonetic matching first with validation
            # try:
                # phonetic_match = self._find_phonetic_match(clean_name)
                # if phonetic_match:
                    # logger.info(f"PHONETIC MATCH FOUND: {clean_name} -> {phonetic_match}")
                    # return phonetic_match, 'phonetic_match'
            # except Exception as e:
                # if config.plugins.m3uconverter.enable_debug.value:
                    # logger.error(f"Phonetic matching failed: {str(e)}")

            # 1. First try to search by exact tvg_id in the Rytec database
            if tvg_id and tvg_id in self.mapping.rytec['basic']:
                service_ref = self.mapping.rytec['basic'][tvg_id]
                if self.is_service_compatible(service_ref):
                    result = service_ref
                    match_type = 'rytec_exact'

            # 2. Search by name in the Rytec database (only if we haven't already found a match)
            if not result:
                best_rytec_score = 0
                for rytec_id, variants in self.mapping.rytec['extended'].items():
                    for variant in variants:
                        if variant['channel_name']:
                            variant_clean = self.clean_channel_name(variant['channel_name'])
                            similarity = self._calculate_similarity(clean_name, variant_clean)
                            if similarity > 0.8 and similarity > best_rytec_score:
                                if self.is_service_compatible(variant['sref']):
                                    best_rytec_score = similarity
                                    result = variant['sref']
                                    match_type = f'rytec_similarity_{int(best_rytec_score * 100)}'

            # 3. FALLBACK: Search in local DVB services (only if we haven't already found a match)
            if not result:
                best_dvb_score = 0
                for db_name, services in self.mapping.dvb.items():
                    similarity = self._calculate_similarity(clean_name, db_name)
                    if similarity > 0.6 and similarity > best_dvb_score:
                        for service in services:
                            if self.is_service_compatible(service['sref']):
                                best_dvb_score = similarity
                                result = service['sref']
                                match_type = f'dvb_fallback_{int(best_dvb_score * 100)}'
                                break

            if result:
                self._add_to_cache_optimized(cache_key, result, match_type)
                return result, match_type
            else:
                return None, 'no_match'
        return self.safe_operation(matching_operation, timeout_seconds=3, default_return=(None, 'timeout'))

    def _add_to_cache(self, cache_key, result, match_type):
        """Optimized cache management with statistics tracking"""
        if result and self.is_service_compatible(result):
            if len(self._match_cache) >= self._cache_max_size:
                # Remove 20% instead of 10% for better performance
                items_to_remove = int(self._cache_max_size * 0.2)
                for key in list(self._match_cache.keys())[:items_to_remove]:
                    del self._match_cache[key]
            self._match_cache[cache_key] = (result, match_type)
        else:
            if result and not self.is_service_compatible(result):
                self._incompatible_matches += 1

    def normalize_service_reference(self, service_ref, for_epg=False):
        """Normalize service reference with correct satellite parameters.

        Args:
            service_ref (str): Service reference to normalize
            for_epg (bool): Whether this is for EPG generation

        Returns:
            str: Normalized service reference
        """
        if not service_ref or not isinstance(service_ref, str):
            return service_ref

        # If it's an IPTV reference, convert it correctly
        if service_ref.startswith('4097:'):
            parts = service_ref.split(':')
            if len(parts) < 11:
                parts += ['0'] * (11 - len(parts))

            # Extract DVB parameters from IPTV reference
            service_type = parts[2]
            service_id = parts[3]
            ts_id = parts[4]
            on_id = parts[5]
            namespace = parts[6]

            # Convert to correct DVB reference
            return f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

        parts = service_ref.split(':')
        if len(parts) < 11:
            parts += ['0'] * (11 - len(parts))

        # Correct satellite parameters
        if len(parts) > 6:
            namespace = parts[6]
            on_id = parts[5]

            # For satellite, ensure ONID and namespace are correct
            if namespace == '820000' and len(on_id) == 1:
                # Probably a terrestrial reference erroneously marked as satellite
                parts[5] = '13E'  # Hotbird position
            elif namespace == '820000' and on_id == '1':
                parts[5] = '13E'  # Correct ONID for Hotbird

        return ':'.join(parts)

    def generate_hybrid_sref(self, dvb_sref, url=None, for_epg=False):
        """Generate correct hybrid service reference.

        Args:
            dvb_sref (str): DVB service reference
            url (str): Stream URL
            for_epg (bool): Whether this is for EPG generation

        Returns:
            str: Hybrid service reference
        """
        if not dvb_sref:
            if url:
                return self.generate_service_reference(url)
            return None

        # If it's already an IPTV reference, use it as is
        if dvb_sref.startswith('4097:'):
            if for_epg:
                # For EPG, convert IPTV -> DVB
                parts = dvb_sref.split(':')
                if len(parts) >= 11:
                    service_type = parts[2]
                    service_id = parts[3]
                    ts_id = parts[4]
                    on_id = parts[5]
                    namespace = parts[6]

                    # Determine correct type based on namespace
                    if namespace == '820000':
                        service_type = '1'  # Satellite
                    elif namespace == 'EEEE':
                        service_type = '16'  # DVB-T
                    elif namespace == 'FFFF':
                        service_type = '10'  # DVB-C

                    return f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"
            return dvb_sref

        dvb_sref = self.normalize_service_reference(dvb_sref, for_epg)

        if for_epg:
            return dvb_sref

        if dvb_sref.startswith('1:0:'):
            parts = dvb_sref.split(':')
            if len(parts) >= 11:
                service_type = parts[2]
                service_id = parts[3]
                ts_id = parts[4]
                on_id = parts[5]
                namespace = parts[6]

                base_sref = f"4097:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

                if url:
                    encoded_url = url.replace(':', '%3a').replace(' ', '%20')
                    return base_sref + encoded_url

                return base_sref

        return self.generate_service_reference(url) if url else None

    def generate_service_reference(self, url):
        """Generate proper IPTV service reference (4097).

        Args:
            url (str): Stream URL

        Returns:
            str: IPTV service reference
        """
        if not url:
            return None

        # Encode URL properly
        encoded_url = url.replace(':', '%3a')
        encoded_url = encoded_url.replace(' ', '%20')
        encoded_url = encoded_url.replace('?', '%3f')
        encoded_url = encoded_url.replace('=', '%3d')
        encoded_url = encoded_url.replace('&', '%26')
        encoded_url = encoded_url.replace('#', '%23')

        # Use a simple counter or timestamp to make it unique
        # For now, we'll use the URL itself to ensure uniqueness
        service_ref = f"4097:0:1:0:0:0:0:0:0:0:{encoded_url}"

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("Generated IPTV service reference: %s", service_ref[:100] + "..." if len(service_ref) > 100 else service_ref)

        return service_ref

    def _calculate_similarity(self, name1, name2):
        """Calculate similarity between two names.

        Args:
            name1 (str): First name
            name2 (str): Second name

        Returns:
            float: Similarity score between 0.0 and 1.0
        """
        if not name1 or not name2:
            return 0.0

        if name1 == name2:
            return 1.0

        # Remove quality indicators
        name1_clean = self._quality_pattern.sub('', name1)
        name2_clean = self._quality_pattern.sub('', name2)

        """
        quality_indicators = ['hd', 'fhd', 'sd', 'uhd', '4k', 'h265', 'hevc', 'h264']
        name1_clean = name1
        name2_clean = name2

        for quality in quality_indicators:
            name1_clean = name1_clean.replace(quality, '')
            name2_clean = name2_clean.replace(quality, '')
        """

        name1_clean = name1_clean.strip()
        name2_clean = name2_clean.strip()
        if name1_clean == name2_clean:
            return 0.8

        try:
            from difflib import SequenceMatcher
            return SequenceMatcher(None, name1_clean, name2_clean).ratio()
        except ImportError:
            common_chars = set(name1_clean) & set(name2_clean)
            return len(common_chars) / max(len(set(name1_clean)), len(set(name2_clean)))

    def verify_epg_generation(self, bouquet_name, expected_channels):
        """Verify EPG generation results.

        Args:
            bouquet_name (str): Name of the bouquet
            expected_channels (int): Expected number of channels

        Returns:
            bool: True if verification successful, False otherwise
        """
        epgimport_path = "/etc/epgimport"
        channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")

        if not fileExists(channels_file):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"EPG channels file not created: {channels_file}")
            return False

        try:
            with open(channels_file, 'r', encoding='utf-8') as f:
                content = f.read()
                actual_channels = content.count('<channel id=')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"EPG verification: Expected {expected_channels}, Got {actual_channels}")

            if actual_channels < expected_channels / 2:  # Less than half
                logger.warning(f"Low EPG coverage: {actual_channels}/{expected_channels}")

            return True

        except Exception as e:
            logger.error(f"Error verifying EPG: {str(e)}")
            return False

    def generate_epg_files(self, bouquet_name, epg_data, epg_url=None):
        """Generate EPG files in /etc/epgimport.

        Args:
            bouquet_name (str): Name of the bouquet
            epg_data (list): EPG data for channels
            epg_url (str): EPG source URL

        Returns:
            bool: True if generation successful, False otherwise
        """
        try:
            epgimport_path = "/etc/epgimport"

            # Ensure directory exists
            if not fileExists(epgimport_path):
                try:
                    mkdir(epgimport_path)
                    logger.info(f"Created EPG directory: {epgimport_path}")
                except Exception as e:
                    logger.error(f"Error creating EPG directory: {str(e)}")
                    return False

            # Generate channels.xml file
            channels_success = self.generate_epg_channels_file(epg_data, bouquet_name)

            # Generate sources.xml file
            if config.plugins.m3uconverter.epg_generation_mode.value == "epgshare":
                sources_success = self.generate_epgshare_sources_file(bouquet_name, epg_url)
            else:
                sources_success = self.generate_epg_sources_file2(bouquet_name, epg_url)

            if channels_success and sources_success:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"EPG files generated correctly in {epgimport_path}")

                # Verify file permissions
                channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
                sources_file = join(epgimport_path, "ArchimedeConverter.sources.xml")

                try:
                    chmod(channels_file, 0o644)
                    chmod(sources_file, 0o644)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("EPG file permissions set correctly")
                except Exception as e:
                    logger.warning(f"Unable to set permissions: {str(e)}")

                return True
            else:
                logger.error("Error generating EPG files")
                return False

        except Exception as e:
            logger.error(f"EPG generation error: {str(e)}")
            return False

    def generate_epg_channels_file(self, epg_data, bouquet_name):
        """Generate channels.xml file with correct service references."""
        epgimport_path = "/etc/epgimport"
        epg_filename = f"{bouquet_name}.channels.xml"
        epg_path = join(epgimport_path, epg_filename)
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Generating EPG channels file with {len(epg_data)} entries")

        try:
            with open(epg_path, 'w', encoding="utf-8", buffering=65536) as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<channels>\n')

                written_count = 0
                for channel in epg_data:
                    # ⚠️ FIX: Define channel_name BEFORE using it
                    channel_name = channel.get('name', 'Unknown')
                    tvg_id = channel.get('tvg_id', '')

                    # ⚠️ FIX: Use the correct SREF
                    service_ref = channel.get('sref', '')

                    # If it’s the IPTV fallback, create a unique SREF
                    if 'iptv_fallback' in service_ref:
                        # Create an SREF based on the channel name
                        import hashlib
                        name_hash = hashlib.md5(channel_name.encode('utf-8')).hexdigest()[:8]
                        service_id = int(name_hash, 16) % 65536
                        service_ref = f"4097:0:1:{service_id}:0:0:0:0:0:0"

                    # Generate correct EPG ID
                    channel_id = self._get_correct_epg_id(channel_name, tvg_id, service_ref)

                    # Write the EPG line
                    line = f'  <!-- {channel_name} --><channel id="{channel_id}">{service_ref}</channel>\n'
                    f.write(line)
                    written_count += 1

                f.write('</channels>\n')

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"EPG channels file created: {written_count} entries")
                logger.info("========= debug_epg_mapping =========")
                self._debug_epg_mapping(epg_data)
            return True

        except Exception as e:
            logger.error(f"Error generating EPG channels file: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _debug_epg_mapping(self, epg_data):
        """Debug per verificare i mapping EPG"""
        if not config.plugins.m3uconverter.enable_debug.value:
            return

        logger.info("=== EPG MAPPING DEBUG ===")
        rytec_count = 0
        dvb_count = 0
        fallback_count = 0

        for i, channel in enumerate(epg_data[:20]):  # Solo primi 20 per debug
            name = channel.get('name', 'Unknown')
            match_type = channel.get('match_type', 'unknown')
            tvg_id = channel.get('tvg_id', '')
            if 'rytec' in match_type:
                rytec_count += 1
            elif 'dvb' in match_type:
                dvb_count += 1
            else:
                fallback_count += 1
            logger.info(f"Channel {i}: {name[:30]} -> {match_type} (tvg_id: {tvg_id})")

        logger.info(f"DEBUG Counts - Rytec: {rytec_count}, DVB: {dvb_count}, Fallback: {fallback_count}")

    def _get_correct_epg_id(self, channel_name, tvg_id=None, service_ref=None):
        """Complete EPG ID matching with all checks.

        Args:
            channel_name (str): Channel name
            tvg_id (str): TV Guide ID
            sref (str): Service reference

        Returns:
            str: Correct EPG ID
        """
        # 1. First priority: use exact Rytec ID if available
        if tvg_id and tvg_id in self.mapping.rytec['basic']:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Using exact Rytec ID: {tvg_id}")
            return tvg_id

        # 2. Search by name in extended Rytec mapping
        if channel_name:
            clean_name = self.clean_channel_name(channel_name)

            # Search for exact matches in Rytec database
            for rytec_id, variants in self.mapping.rytec['extended'].items():
                for variant in variants:
                    if variant['channel_name'] and self.clean_channel_name(variant['channel_name']) == clean_name:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Found Rytec match by name: {channel_name} -> {rytec_id}")
                        return rytec_id

            # Search for partial matches
            for rytec_id, variants in self.mapping.rytec['extended'].items():
                for variant in variants:
                    if variant['channel_name']:
                        rytec_clean_name = self.clean_channel_name(variant['channel_name'])
                        similarity = self._calculate_similarity(clean_name, rytec_clean_name)
                        if similarity > 0.8:  # High threshold for matching
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.debug(f"Found high similarity match: {channel_name} -> {rytec_id} (score: {similarity})")
                            return rytec_id

        # 3. Fallback: generate Rytec-style ID
        fallback_id = self._generate_rytec_style_id(channel_name, service_ref)
        logger.debug(f"Using fallback ID: {fallback_id} for {channel_name}")
        return fallback_id

    def generate_epg_sources_file(self, bouquet_name, epg_url=None):
        """Generate sources.xml that correctly points to the channels files."""
        epgimport_path = "/etc/epgimport"
        sources_path = join(epgimport_path, "ArchimedeConverter.sources.xml")
        try:
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

            # Create the new correct source
            new_source = '    <source type="gen_xmltv" nocheck="1" channels="%s.channels.xml">\n' % bouquet_name
            new_source += '      <description>%s</description>\n' % bouquet_name

            if epg_url:
                new_source += '      <url><![CDATA[%s]]></url>\n' % epg_url
            else:
                # Add default URLs based on language
                language_code = self._get_system_country_code().upper()
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

    def generate_epgshare_sources_file(self, bouquet_name, epg_url=None):
        """Generate sources.xml for EPGShare mode.

        Args:
            bouquet_name (str): Name of the bouquet
            epg_url (str): EPG source URL

        Returns:
            bool: True if generation successful, False otherwise
        """
        epgimport_path = "/etc/epgimport"
        sources_path = join(epgimport_path, "ArchimedeConverter.sources.xml")
        try:
            if not fileExists(epgimport_path):
                mkdir(epgimport_path)

            # If epg_url not provided, use language-based URL
            if not epg_url:
                language = config.plugins.m3uconverter.language.value
                epg_url = self.get_epg_url_for_language(language)

            content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n'
            content += '  <sourcecat sourcecatname="Archimede Converter">\n'
            content += f'    <source type="gen_xmltv" nocheck="1" channels="{bouquet_name}.channels.xml">\n'
            content += f'      <description>{bouquet_name}</description>\n'
            content += f'      <url><![CDATA[{epg_url}]]></url>\n'
            content += '    </source>\n'
            content += '  </sourcecat>\n'
            content += '</sources>'

            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Generated EPG sources with URL: {epg_url}")
            return True

        except Exception as e:
            logger.error(f"Error generating EPG sources: {str(e)}")
            return False

    def generate_epg_sources_file2(self, bouquet_name, epg_url=None, mode="append"):
        """Dynamic EPG sources management for multiple bouquets.

        Args:
            bouquet_name (str): Name of the bouquet
            epg_url (str): EPG source URL
            mode (str): Operation mode ("append" or "replace")

        Returns:
            bool: True if generation successful, False otherwise
        """
        epgimport_path = "/etc/epgimport"
        sources_filename = "ArchimedeConverter.sources.xml"
        sources_path = join(epgimport_path, sources_filename)

        try:
            if not fileExists(epgimport_path):
                try:
                    mkdir(epgimport_path)
                except Exception as e:
                    print(e)
                    return False

            # Get language and URLs
            language_code = "ALL"
            try:
                if hasattr(config.plugins, 'm3uconverter') and hasattr(config.plugins.m3uconverter, 'language'):
                    language_code = config.plugins.m3uconverter.language.value.upper()
            except:
                pass

            urls = self._get_epg_urls_for_language(language_code)

            # Read existing content or create new
            if fileExists(sources_path) and mode == "append":
                with open(sources_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n</sources>'

            # Create source for this bouquet
            bouquet_source = [
                f'    <source type="gen_xmltv" nocheck="1" channels="{bouquet_name}.channels.xml">',
                f'      <description>{bouquet_name}</description>'
            ]
            for url in urls:
                bouquet_source.append(f'      <url>{url}</url>')
            bouquet_source.append('    </source>')

            new_source = '\n'.join(bouquet_source) + '\n'

            # Check if sourcecat exists
            sourcecat_pattern = r'<sourcecat sourcecatname="Archimede Converter by Lululla">(.*?)</sourcecat>'
            sourcecat_match = search(sourcecat_pattern, content, DOTALL)

            if sourcecat_match:
                # Remove existing source for this bouquet if exists
                existing_pattern = rf'<source type="gen_xmltv"[^>]*channels="{escape(bouquet_name)}\.channels\.xml"[^>]*>.*?</source>'
                content = sub(existing_pattern, '', content, flags=DOTALL)

                # Add new source
                existing_content = sourcecat_match.group(1)
                updated_content = existing_content + '\n' + new_source
                content = content.replace(sourcecat_match.group(0),
                                          f'<sourcecat sourcecatname="Archimede Converter by Lululla">{updated_content}</sourcecat>')
            else:
                # Create new sourcecat
                new_sourcecat = '  <sourcecat sourcecatname="Archimede Converter by Lululla">\n'
                new_sourcecat += new_source
                new_sourcecat += '  </sourcecat>\n'
                content = content.replace('</sources>', new_sourcecat + '</sources>')

            # Ensure rytec source is always present
            rytec_source = [
                '    <source type="gen_xmltv" nocheck="1" channels="rytec.channels.xml">',
                f'      <description>Rytec Channels ({language_code})</description>'
            ]
            for url in urls:
                rytec_source.append(f'      <url>{url}</url>')
            rytec_source.append('    </source>')

            rytec_source_str = '\n'.join(rytec_source) + '\n'

            # Add rytec source if not present
            rytec_pattern = r'<source type="gen_xmltv"[^>]*channels="rytec\.channels\.xml"[^>]*>.*?</source>'
            if not search(rytec_pattern, content, DOTALL):
                sourcecat_match = search(sourcecat_pattern, content, DOTALL)
                if sourcecat_match:
                    existing_content = sourcecat_match.group(1)
                    updated_content = rytec_source_str + existing_content
                    content = content.replace(sourcecat_match.group(0),
                                              f'<sourcecat sourcecatname="Archimede Converter by Lululla">{updated_content}</sourcecat>')

            # Write back
            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Updated EPG sources for bouquet: {bouquet_name}")
            return True

        except Exception as e:
            logger.error(f"Error in dynamic EPG sources: {str(e)}")
            return False

    def _get_epg_urls_for_language(self, language_code):
        """Get EPG URLs based on language selection.

        Args:
            language_code (str): Two-letter language code

        Returns:
            list: List of EPG URLs
        """
        # Comprehensive language to source mapping
        language_to_sources = {
            'ALL': [
                'https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz'
                # 'http://epg-guide.com/epg-guide.channels.xz'
            ],
            'IT': [
                'https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz',
                'http://epg-guide.com/epg-guide.channels.xz',
                'http://epg-guide.com/dttsat.xz',
                'http://www.xmltvepg.nl/rytecIT_Sky.xz'
            ],
            'EN': [
                'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz',
                'https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz'
            ],
            'DE': [
                'https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz'
            ],
            'FR': [
                'https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz'
            ],
            'ES': [
                'https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz'
            ],
            'NL': [
                'https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz'
            ],
            'PL': [
                'https://epgshare01.online/epgshare01/epg_ripper_PL1.xml.gz'
            ],
            'GR': [
                'https://epgshare01.online/epgshare01/epg_ripper_GR1.xml.gz'
            ],
            'CZ': [
                'https://epgshare01.online/epgshare01/epg_ripper_CZ1.xml.gz'
            ],
            'HU': [
                'https://epgshare01.online/epgshare01/epg_ripper_HU1.xml.gz'
            ],
            'RO': [
                'https://epgshare01.online/epgshare01/epg_ripper_RO1.xml.gz'
            ],
            'SE': [
                'https://epgshare01.online/epgshare01/epg_ripper_SE1.xml.gz'
            ],
            'NO': [
                'https://epgshare01.online/epgshare01/epg_ripper_NO1.xml.gz'
            ],
            'DK': [
                'https://epgshare01.online/epgshare01/epg_ripper_DK1.xml.gz'
            ],
            'FI': [
                'https://epgshare01.online/epgshare01/epg_ripper_FI1.xml.gz'
            ]
        }

        return language_to_sources.get(language_code, language_to_sources['ALL'])

    def _download_epg_file(self, url, output_path):
        """Download and decompress EPG file.

        Args:
            url (str): EPG file URL
            output_path (str): Output file path

        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            import requests
            import gzip
            from io import BytesIO
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/xml, */*"
            }

            # Download the file
            response = requests.get(url, headers=headers, timeout=30, verify=False)
            response.raise_for_status()

            # Check if it's a gzipped file
            if url.endswith('.gz') or response.headers.get('content-type') == 'application/gzip':
                # Decompress gzip file
                with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz_file:
                    decompressed_content = gz_file.read()

                # Write decompressed content
                with open(output_path, 'wb') as f:
                    f.write(decompressed_content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Downloaded and decompressed EPG file to {output_path}")

            else:
                # Write directly if not compressed
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Downloaded EPG file to {output_path}")

            return True

        except Exception as e:
            logger.error(f"EPG download failed: {str(e)}")
            return False

    def download_and_parse_epgshare(self, language_code="all"):
        """Download and parse EPGShare data with extensive debugging.

        Args:
            language_code (str): Language code for EPG source

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            country_code = LANGUAGE_TO_COUNTRY.get(language_code, 'ALL')
            epg_url = f"https://epgshare01.online/epgshare01/epg_ripper_{country_code}1.xml.gz"
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Downloading EPG from: {epg_url}")

            # Download the file
            temp_path = "/tmp/epgshare_download.xml"
            success = self._download_epg_file(epg_url, temp_path)

            if not success:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPGShare download failed")
                return False

            # Parse the file
            parse_success = self._parse_epgshare_for_mapping(temp_path)

            if parse_success:
                # Verify results
                epgshare_count = 0
                with self._rytec_lock:
                    for channel_id, variants in self.mapping.rytec['extended'].items():
                        for variant in variants:
                            if variant.get('source_type') == 'epgshare':
                                epgshare_count += 1
                                break
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"EPGShare parsing completed: {epgshare_count} channels")

                if epgshare_count > 0:
                    return True
                else:
                    logger.error("Parsing succeeded but no channels were added!")
                    return False
            else:
                logger.error("EPGShare parsing failed")
                return False

        except Exception as e:
            logger.error(f"EPG Share error: {str(e)}")
            return False

    def _parse_epgshare_for_mapping(self, epg_path):
        """Robust EPGShare parsing with lxml.

        Args:
            epg_path (str): Path to EPG file

        Returns:
            bool: True if parsing successful, False otherwise
        """
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Parsing EPGShare file: {epg_path}")

            # Clear existing EPGShare entries
            self._clear_epgshare_entries()

            # Check file exists and has content
            if not fileExists(epg_path):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPGShare file does not exist")
                return False

            file_size = getsize(epg_path)
            if file_size == 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPGShare file is empty")
                return False
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"File size: {file_size} bytes")

            # Parse with lxml
            if LXML_AVAILABLE:
                return self._parse_with_lxml(epg_path)
            else:
                return self._parse_with_elementtree(epg_path)

        except Exception as e:
            logger.error(f"EPGShare parsing error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _clear_epgshare_entries(self):
        """Clear all EPGShare entries"""
        keys_to_remove = []
        for channel_id, variants in self.mapping.rytec['extended'].items():
            for variant in variants:
                if variant.get('source_type') == 'epgshare':
                    keys_to_remove.append(channel_id)
                    break

        for channel_id in keys_to_remove:
            del self.mapping.rytec['extended'][channel_id]
            if channel_id in self.mapping.rytec['basic']:
                del self.mapping.rytec['basic'][channel_id]
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Cleared {len(keys_to_remove)} EPGShare entries")

    def _parse_with_lxml(self, epg_path):
        """Parse with lxml library.

        Args:
            epg_path (str): Path to EPG file

        Returns:
            bool: True if parsing successful, False otherwise
        """
        try:
            parser = etree.XMLParser(encoding='utf-8', recover=True)
            tree = etree.parse(epg_path, parser)
            root = tree.getroot()

            if root is None:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Failed to get root element")
                return False

            # Find all channel elements
            channels = root.findall('.//channel')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Found {len(channels)} channel elements with lxml")

            added_count = 0
            for channel in channels:
                try:
                    channel_id = channel.get('id')
                    if not channel_id:
                        continue

                    # Find display name
                    display_name_elem = channel.find('display-name')
                    if display_name_elem is None or not display_name_elem.text:
                        continue

                    display_name = display_name_elem.text.strip()

                    # Add to mapping
                    clean_name = self.clean_channel_name(display_name, preserve_variants=True)

                    self.mapping.rytec['extended'][channel_id] = [{
                        'channel_name': display_name,
                        'sref': None,
                        'source_type': 'epgshare',
                        'original_id': channel_id,
                        'clean_name': clean_name
                    }]

                    self.mapping.rytec['basic'][channel_id] = None
                    added_count += 1

                except Exception as e:
                    logger.warning(f"Error parsing channel: {str(e)}")
                    continue
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Added {added_count} channels with lxml parsing")
            return added_count > 0

        except Exception as e:
            logger.error(f"lxml parsing failed: {str(e)}")
            return False

    def _parse_with_elementtree(self, epg_path):
        """Parse with ElementTree fallback.

        Args:
            epg_path (str): Path to EPG file

        Returns:
            bool: True if parsing successful, False otherwise
        """
        try:
            tree = ET.parse(epg_path)
            root = tree.getroot()

            channels = root.findall('.//channel')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Found {len(channels)} channel elements with ElementTree")

            added_count = 0
            for channel in channels:
                try:
                    channel_id = channel.get('id')
                    if not channel_id:
                        continue

                    display_name_elem = channel.find('display-name')
                    if display_name_elem is None or not display_name_elem.text:
                        continue

                    display_name = display_name_elem.text.strip()

                    clean_name = self.clean_channel_name(display_name, preserve_variants=True)

                    self.mapping.rytec['extended'][channel_id] = [{
                        'channel_name': display_name,
                        'sref': None,
                        'source_type': 'epgshare',
                        'original_id': channel_id,
                        'clean_name': clean_name
                    }]

                    self.mapping.rytec['basic'][channel_id] = None
                    added_count += 1

                except Exception as e:
                    logger.warning(f"Error parsing channel: {str(e)}")
                    continue
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Added {added_count} channels with ElementTree parsing")
            return added_count > 0

        except Exception as e:
            logger.error(f"ElementTree parsing failed: {str(e)}")
            return False

    def _find_epgshare_match(self, clean_name, original_name, tvg_id):
        """Optimized search for EPGShare mode - NAME BASED ONLY.

        Args:
            clean_name (str): Cleaned channel name
            original_name (str): Original channel name
            tvg_id (str): TV Guide ID

        Returns:
            tuple: (service_reference, match_type)
        """
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"EPGShare search: {clean_name}, original: {original_name}, tvg_id: {tvg_id}")

            best_match = None
            best_score = 0
            best_match_type = 'no_match'

            # Search in EPGShare database
            for channel_id, variants in self.mapping.rytec['extended'].items():
                for variant in variants:
                    if variant.get('source_type') == 'epgshare':
                        variant_clean = variant.get('clean_name', '')

                        if not variant_clean:
                            continue

                        # Exact match on clean names
                        if variant_clean == clean_name:
                            local_sref = self._find_service_in_local_database(clean_name)
                            if local_sref and self.is_service_compatible(local_sref):
                                return local_sref, 'epgshare_exact'

                        # Similarity match
                        similarity = self._calculate_similarity(clean_name, variant_clean)
                        if similarity > 0.8 and similarity > best_score:
                            local_sref = self._find_service_in_local_database(clean_name)
                            if local_sref and self.is_service_compatible(local_sref):
                                best_match = local_sref
                                best_score = similarity
                                best_match_type = f'epgshare_similarity_{int(similarity * 100)}'

            if best_match:
                return best_match, best_match_type

            return None, 'no_match'

        except Exception as e:
            logger.error(f"Error in _find_epgshare_match: {str(e)}")
            return None, 'error'

    def _find_service_in_local_database(self, clean_name):
        """Find service reference in local databases (lamedb + bouquet).

        Args:
            clean_name (str): Cleaned channel name

        Returns:
            str: Service reference or fallback
        """
        # 1. Search in bouquets (highest priority)
        if clean_name in self.mapping.dvb:
            for service in self.mapping.dvb[clean_name]:
                if service['source'] == 'bouquet' and self.is_service_compatible(service['sref']):
                    return service['sref']

        # 2. Search in lamedb
        if clean_name in self.mapping.dvb:
            for service in self.mapping.dvb[clean_name]:
                if service['source'] == 'lamedb' and self.is_service_compatible(service['sref']):
                    return service['sref']

        # 3. Search in optimized mapping
        if clean_name in self.mapping.optimized:
            service = self.mapping.optimized[clean_name]
            if self.is_service_compatible(service['sref']):
                return service['sref']

        # 4. Fallback: generate IPTV reference
        return self.generate_service_reference("iptv_fallback")

    def extract_epg_url_from_m3u(self, m3u_path):
        """Search for an EPG URL in M3U file comments.

        Args:
            m3u_path (str): Path to M3U file

        Returns:
            str: EPG URL or None if not found
        """
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

    def _create_fallback_mapping_from_dvb(self):
        """Create fallback EPG mapping from existing DVB services."""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("Creating fallback mapping from DVB services...")

        count = 0
        with self._rytec_lock:
            for clean_name, services in self.mapping.dvb.items():
                if services and count < 1000:  # Limit to 1000 channels
                    service = services[0]
                    if service['sref'] and self.is_service_compatible(service['sref']):
                        # Generate Rytec-style ID
                        channel_id = self._generate_rytec_style_id(clean_name, service['sref'])

                        self.mapping.rytec['extended'][channel_id].append({
                            'sref': service['sref'],
                            'channel_name': clean_name,
                            'source_type': 'dvb_fallback',
                            'original_id': channel_id
                        })

                        self.mapping.rytec['basic'][channel_id] = service['sref']
                        count += 1

        logger.info(f"Created fallback mapping with {count} DVB channels")

    def initialize(self):
        """Initialize with focus on Rytec loading."""
        try:
            logger.info("🎯 INITIALIZING EPG SERVICE MAPPER")
            
            # 1. Load DVB services
            self.parse_lamedb()
            self.parse_existing_bouquets()
            logger.info(f"📡 DVB services loaded: {len(self.mapping.dvb)}")
            
            # 2. FORCE Rytec loading with debug
            logger.info("🔍 LOADING RYTEC DATABASE...")
            rytec_paths = [
                "/etc/epgimport/rytec.channels.xml",
                "/usr/lib/enigma2/python/Plugins/Extensions/EPGImport/rytec.channels.xml",
            ]
            
            rytec_loaded = False
            for rytec_path in rytec_paths:
                if fileExists(rytec_path):
                    logger.info(f"📁 Rytec file found: {rytec_path}")
                    self.parse_rytec_channels(rytec_path)
                    if len(self.mapping.rytec['extended']) > 0:
                        rytec_loaded = True
                        logger.info(f"✅ Rytec database loaded: {len(self.mapping.rytec['extended'])} channels")
                        break
                else:
                    logger.warning(f"📁 File not found: {rytec_path}")
            
            if not rytec_loaded:
                logger.error("❌ CRITICAL: No Rytec database loaded!")
                logger.info("💡 Solution: Install EPGImport or manually download rytec.channels.xml")
            
            # 3. EPGShare mode (only if Rytec not loaded)
            if not rytec_loaded and config.plugins.m3uconverter.epg_generation_mode.value == "epgshare":
                logger.info("🌐 Attempting to load EPGShare...")
                language = config.plugins.m3uconverter.language.value
                if self.download_and_parse_epgshare(language):
                    logger.info("✅ EPGShare loaded as fallback")
            
            # 4. Final statistics
            logger.info("📊 FINAL STATISTICS:")
            logger.info(f"   Rytec channels: {len(self.mapping.rytec['extended'])}")
            logger.info(f"   DVB channels: {len(self.mapping.dvb)}")
            
            if len(self.mapping.rytec['extended']) == 0:
                logger.error("🚨 WARNING: Rytec database is empty! EPG will not work!")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Initialization failed: {str(e)}")
            return False

    def initializeOOOOOOOOOOO(self):
        """Initialize with correct priority system.

        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            logger.info("=== INITIALIZATION WITH CORRECT PRIORITY ===")
            logger.info(f"EPG Mode: {config.plugins.m3uconverter.epg_generation_mode.value}")

            # 1. Always load lamedb and bouquets (fundamental)
            self.parse_lamedb()
            self.parse_existing_bouquets()
            self.load_channel_mapping()

            # 2. STANDARD mode, load ONLY local database
            if config.plugins.m3uconverter.epg_generation_mode.value == "standard":
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("🎯 MODE: STANDARD - Using local databases")
                local_rytec = "/etc/epgimport/rytec.channels.xml"
                if fileExists(local_rytec):
                    self.parse_rytec_channels(local_rytec)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"✅ Local Rytec database loaded: {len(self.mapping.rytec['extended'])} channels")
                else:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning("⚠️ Rytec file not found: %s", local_rytec)
                    self._create_fallback_mapping_from_dvb()

            # 3. EPGSHARE mode
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("🎯 MODE: EPGSHARE - Download from epgshare01")
                language = config.plugins.m3uconverter.language.value
                self.download_and_parse_epgshare(language)

            # 4. Optimize data structures
            self.optimize_matching()

            # 5. Setup memory optimization timer
            self.optimize_memory_timer = eTimer()
            self.optimize_memory_timer.callback.append(self.optimize_memory_usage)
            self.optimize_memory_timer.start(30000)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Initialization completed correctly (with memory optimization)")

            return True

        except Exception as e:
            import traceback
            logger.error(f"Initialization failed: {str(e)}")
            logger.debug(traceback.format_exc())
            return False

    def optimize_memory_usage(self):
        """Periodic memory cleanup"""
        try:
            # Clearing match cache if too large
            if len(self._match_cache) > self._cache_max_size * 1.2:
                excess = len(self._match_cache) - self._cache_max_size
                keys_to_remove = list(self._match_cache.keys())[:excess]
                for key in keys_to_remove:
                    del self._match_cache[key]
                logger.debug(f"Cleaned {excess} entries from match cache")

            # Clean cache EPG
            if len(self._clean_name_cache) > self._clean_cache_max_size:
                self._clean_name_cache.clear()
                logger.debug("Cleaned clean name cache")

        except Exception as e:
            logger.error(f"Memory optimization error: {str(e)}")

    def safe_operation(self, operation, timeout_seconds=5, default_return=None):
        """Performs an operation with a timeout to prevent freezing."""
        try:
            import threading
            import queue

            result_queue = queue.Queue()

            def worker():
                try:
                    result = operation()
                    result_queue.put(('success', result))
                except Exception as e:
                    result_queue.put(('error', e))

            thread = threading.Thread(target=worker, daemon=True)
            thread.start()
            thread.join(timeout=timeout_seconds)

            if thread.is_alive():
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning(f"Operation timed out after {timeout_seconds} seconds")
                return default_return

            if not result_queue.empty():
                status, value = result_queue.get()
                if status == 'success':
                    return value
                else:
                    raise value

            return default_return

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Safe operation failed: {str(e)}")
            return default_return

    def verify_epg_files(self, bouquet_name):
        """Verify that EPG files were created correctly.

        Args:
            bouquet_name (str): Name of the bouquet

        Returns:
            bool: True if verification successful, False otherwise
        """
        epgimport_path = "/etc/epgimport"
        channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
        sources_file = join(epgimport_path, "ArchimedeConverter.sources.xml")

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("=== EPG FILE VERIFICATION ===")

        # Verify file existence
        channels_exists = fileExists(channels_file)
        sources_exists = fileExists(sources_file)

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Channels file: {channels_exists} - {channels_file}")
            logger.info(f"Sources file: {sources_exists} - {sources_file}")

        # Verify content
        channel_count = 0
        if channels_exists:
            try:
                with open(channels_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    channel_count = content.count('<channel id=')
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Channels in file: {channel_count}")

                    # Count match types
                    rytec_matches = content.count('rytec_')
                    dvb_matches = content.count('dvb_')
                    fallback_matches = content.count('iptv_fallback')

                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Rytec matches: {rytec_matches}, DVB matches: {dvb_matches}, Fallback: {fallback_matches}")

            except Exception as e:
                logger.error(f"Error reading channels.xml: {str(e)}")

        # Verify sources
        if sources_exists:
            try:
                with open(sources_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    has_bouquet = bouquet_name in content
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Bouquet present in sources: {has_bouquet}")

            except Exception as e:
                logger.error(f"Error reading sources.xml: {str(e)}")

        return channels_exists and sources_exists and channel_count > 0

    def _debug_epg_failure(self, bouquet_name, epg_data):
        """Additional debug for EPG failures.

        Args:
            bouquet_name (str): Name of the bouquet
            epg_data (list): EPG data
        """
        if config.plugins.m3uconverter.enable_debug.value:
            logger.error("=== EPG GENERATION FAILURE DEBUG ===")
            logger.error(f"Bouquet name: {bouquet_name}")
            logger.error(f"EPG data entries: {len(epg_data)}")

        # Check first 10 channels
        for i, channel in enumerate(epg_data[:10]):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Channel {i}: {channel.get('name')}")
                logger.error(f"  SREF: {channel.get('sref')}")
                logger.error(f"  TVG_ID: {channel.get('tvg_id')}")

        # Check directory permissions
        epgimport_path = "/etc/epgimport"
        if fileExists(epgimport_path):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"EPG import path exists: {epgimport_path}")
                logger.error(f"Write permission: {access(epgimport_path, W_OK)}")
        else:
            logger.error(f"EPG import path does not exist: {epgimport_path}")

    def _debug_mapping_sources(self):
        """Debug where service references come from."""
        if not config.plugins.m3uconverter.enable_debug.value:
            return
        logger.info("=== MAPPING SOURCES DEBUG ===")

        # Count channels by type
        rytec_count = len(self.mapping.rytec['extended'])
        rytec_basic = len(self.mapping.rytec['basic'])

        # Count EPGShare channels specifically
        epgshare_count = 0
        for channel_id, variants in self.mapping.rytec['extended'].items():
            for variant in variants:
                if variant.get('source_type') == 'epgshare':
                    epgshare_count += 1
                    break
        logger.info(f"Rytec Extended: {rytec_count} channels")
        logger.info(f"Rytec Basic: {rytec_basic} channels")
        logger.info(f"EPGShare: {epgshare_count} channels")

        if epgshare_count == 0 and config.plugins.m3uconverter.epg_generation_mode.value == "epgshare":
            logger.warning("WARNING: No EPGShare channels loaded!")
            logger.warning("Check EPGShare download and parsing")

        # Log first 10 EPGShare channels for debug
        if epgshare_count > 0:
            logger.info("First 10 EPGShare channels:")
            count = 0
            for channel_id, variants in self.mapping.rytec['extended'].items():
                for variant in variants:
                    if variant.get('source_type') == 'epgshare' and count < 10:
                        logger.info(f"   {count + 1}. {channel_id} -> {variant.get('channel_name', 'Unknown')}")
                        count += 1
                        break

        source_stats = defaultdict(int)
        for services in self.mapping.dvb.values():
            for service in services:
                source_stats[service['source']] += 1
        logger.info(f"📊 DVB Sources: {dict(source_stats)}")
        logger.info(f"📊 Rytec Extended: {len(self.mapping.rytec['extended'])}")
        logger.info(f"📊 Optimized: {len(self.mapping.optimized)}")


class ChannelCache:
    def __init__(self, max_size=5000):
        self._cache = {}
        self._max_size = max_size
        self._hits = 0
        self._misses = 0

    def get_clean_name(self, name):
        if name in self._cache:
            self._hits += 1
            return self._cache[name]

        self._misses += 1
        # Pulizia base senza regex complesse
        cleaned = name.lower().strip()
        for char in '[]()|':
            cleaned = cleaned.replace(char, ' ')
        cleaned = ' '.join(cleaned.split())

        # Gestione cache size
        if len(self._cache) >= self._max_size:
            # Rimuovi il 10% più vecchio
            keys_to_remove = list(self._cache.keys())[:self._max_size // 10]
            for key in keys_to_remove:
                del self._cache[key]

        self._cache[name] = cleaned
        return cleaned


class CoreConverter:
    """Main converter class with backup and logging functionality."""

    _instance = None
    _lock = Lock()

    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        """Initialize core converter with directories and logging."""
        if not self.__initialized:
            self.backup_dir = join(ARCHIMEDE_CONVERTER_PATH, "archimede_backup")
            self.log_file = join(ARCHIMEDE_CONVERTER_PATH, "archimede_converter.log")
            self._create_necessary_directories()
            self.__initialized = True

    def _create_necessary_directories(self):
        """Create necessary directories if they don't exist."""
        try:
            makedirs(self.backup_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Error creating directories: {str(e)}")

    def safe_conversion(self, function, *args, **kwargs):
        """Perform conversion with automatic backup and error handling.

        Args:
            function: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function execution

        Raises:
            RuntimeError: If conversion fails
        """
        try:
            self._create_backup()
            result = function(*args, **kwargs)
            self._log_success(function.__name__)
            return result
        except Exception as e:
            self._log_error(e)
            self._restore_backup()
            raise RuntimeError(f"Conversion failed (restored backup). Error: {str(e)}")

    def _create_backup(self):
        """Create a backup of the existing bouquets."""
        try:
            if not exists("/etc/enigma2/bouquets.tv"):
                return

            timestamp = strftime("%Y%m%d_%H%M%S")
            backup_file = join(self.backup_dir, f"bouquets_{timestamp}.tv")
            shutil.copy2("/etc/enigma2/bouquets.tv", backup_file)
        except Exception as e:
            raise RuntimeError(f"Backup failed: {str(e)}")

    def _restore_backup(self):
        """Restore the most recent available backup."""
        try:
            backups = sorted([f for f in listdir(self.backup_dir)
                              if f.startswith("bouquets_") and f.endswith(".tv")])

            if backups:
                latest_backup = join(self.backup_dir, backups[-1])
                shutil.copy2(latest_backup, "/etc/enigma2/bouquets.tv")
        except Exception as e:
            raise RuntimeError(f"Restore failed: {str(e)}")

    def _log_success(self, operation_name):
        """Log a successful operation.

        Args:
            operation_name (str): Name of the successful operation
        """
        message = f"{strftime('%Y-%m-%d %H:%M:%S')} [SUCCESS] {operation_name}"
        self._write_to_log(message)

    def _log_error(self, error):
        """Log an error.
        Args:
            error: Error object or message
        """
        message = f"{strftime('%Y-%m-%d %H:%M:%S')} [ERROR] {str(error)}"
        self._write_to_log(message)

    def _write_to_log(self, message):
        """Write message to log file.

        Args:
            message (str): Message to log
        """
        try:
            with open(self.log_file, "a") as f:
                f.write(message + "\n")
        except Exception:
            print(f"Fallback log: {message}")

    def filter_channels_by_type(self, channels, filter_type="all"):
        """Filter channels by type.

        Args:
            channels (list): List of channel dictionaries
            filter_type (str): Type of filter to apply

        Returns:
            list: Filtered list of channels
        """
        if not channels:
            return []

        if filter_type == "working":
            return [ch for ch in channels if self._is_url_accessible(ch.get("url", ""))]
        return channels

    def _is_url_accessible(self, url, timeout=5):
        """Check if a URL is reachable.

        Args:
            url (str): URL to check
            timeout (int): Timeout in seconds

        Returns:
            bool: True if URL is accessible, False otherwise
        """
        if not url:
            return False

        try:
            cmd = f"curl --max-time {timeout} --head --silent --fail --output /dev/null {url}"
            return system(cmd) == 0
        except Exception:
            return False

    def cleanup_old_backups(self, max_backups=5):
        """Keep only the latest N backups.

        Args:
            max_backups (int): Maximum number of backups to keep
        """
        try:
            backups = sorted([f for f in listdir(self.backup_dir)
                              if f.startswith("bouquets_") and f.endswith(".tv")])

            for old_backup in backups[:-max_backups]:
                remove(join(self.backup_dir, old_backup))
        except Exception as e:
            self._log_error(f"Cleanup failed: {str(e)}")


# ==================== SCREEN CLASSES ====================


class M3UFileBrowser(Screen):
    """File browser screen for selecting M3U, TV, JSON, and XSPF files."""

    def __init__(self, session, startdir="/etc/enigma2",
                 matchingPattern=r"(?i)^.*\.(tv|m3u|m3u8|json|xspf)$",
                 conversion_type=None, title=None):
        """Initialize file browser.

        Args:
            session: Enigma2 session
            startdir (str): Starting directory
            matchingPattern (str): File pattern to match
            conversion_type (str): Type of conversion
            title (str): Screen title
        """
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
            "ok": self._on_ok_pressed,
            "green": self._on_ok_pressed,
            "cancel": self.close
        }, -1)
        if self.conversion_type == "tv_to_m3u":
            self.onShown.append(self._filter_file_list)

    def _filter_file_list(self):
        """Filter list to show only directories and .tv files containing 'http'."""
        filtered_entries = []
        for entry in self["filelist"].list:
            if not entry or not isinstance(entry[0], tuple):
                continue

            file_data = entry[0]
            path = None
            is_directory = False

            if len(file_data) >= 2:
                path = file_data[0]
                is_directory = file_data[1]

            elif len(file_data) == 1 and isinstance(file_data[0], str):
                path = file_data[0]
                is_directory = True
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Skipping invalid entry: {file_data}")
                continue

            if path == ".." or is_directory:
                filtered_entries.append(entry)
            else:
                if path and path.lower().endswith(".tv") and self._file_contains_http(path):
                    filtered_entries.append(entry)

        self["filelist"].list = filtered_entries
        self["filelist"].l.setList(filtered_entries)

    def _file_contains_http(self, filename):
        """Check if file contains 'http' (case-insensitive).

        Args:
            filename (str): Name of file to check

        Returns:
            bool: True if file contains HTTP references
        """
        try:
            current_directory = self["filelist"].getCurrentDirectory()
            full_path = join(current_directory, filename)

            with open(full_path, "r") as f:
                return any("http" in line.lower() for line in f)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error reading {full_path}: {str(e)}")
            return False

    def _on_ok_pressed(self):
        """Handle OK button press for file selection."""
        selection = self["filelist"].getCurrent()
        if not selection or not isinstance(selection, list) or not isinstance(selection[0], tuple):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Invalid selection format: {selection}")
            return

        file_data = selection[0]
        path = file_data[0]
        is_directory = file_data[1]

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"file_data: {file_data}, path: {path}, is_directory: {is_directory}")
        try:
            if is_directory:
                self["filelist"].changeDir(path)
                if self.conversion_type == "tv_to_m3u":
                    self._filter_file_list()
            else:
                current_directory = self["filelist"].getCurrentDirectory()
                full_path = join(current_directory, path)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Selected full file path: {full_path}")
                self.close(full_path)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error in OK pressed: {str(e)}")

    def close(self, result=None):
        """Close the file browser.

        Args:
            result: Result to return
        """
        try:
            super(M3UFileBrowser, self).close(result)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error closing browser: {str(e)}")
            super(M3UFileBrowser, self).close(None)


class ConversionSelector(Screen):
    """Main conversion selector screen."""
    skin = """
        <screen name="ConversionSelector" position="center,center" size="1280,720" title="..::ConversionSelector::.." backgroundColor="#20000000" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
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
        """Initialize conversion selector screen.

        Args:
            session: Enigma2 session
        """
        Screen.__init__(self, session)
        self.session = session
        self.skinName = "ConversionSelector"
        self.is_modal = True
        self.setTitle(PLUGIN_TITLE)
        self.menu_options = [
            (_("M3U ➔ Enigma2 Bouquets"), "m3u_to_tv", "m3u"),
            (_("Enigma2 Bouquets ➔ M3U"), "tv_to_m3u", "tv"),
            (_("JSON ➔ Enigma2 Bouquets"), "json_to_tv", "json"),
            (_("JSON ➔ M3U"), "json_to_m3u", "json"),
            (_("XSPF ➔ M3U Playlist"), "xspf_to_m3u", "xspf"),
            (_("M3U ➔ JSON"), "m3u_to_json", "m3u"),
            (_("Remove M3U Bouquets"), "purge_m3u_bouquets", None)
        ]

        self["list"] = MenuList([(option[0], option[1]) for option in self.menu_options])
        self["Title"] = Label(PLUGIN_TITLE)
        self["info"] = Label('')
        self["text"] = Label('')
        self["status"] = Label(_("We're ready: what do you want to do?"))

        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MenuActions"], {
            "red": self.close,
            "green": self._select_current_item,
            "blue": self._open_epg_importer,
            "menu": self._open_settings,
            "ok": self._select_current_item,
            "yellow": self._purge_m3u_bouquets,
            "cancel": self.close
        })

        self["key_red"] = StaticText(_("Close"))
        self["key_green"] = StaticText(_("Select"))
        self["key_yellow"] = StaticText(_("Remove Bouquets"))
        self["key_blue"] = StaticText(_("EPGImporter"))

    def _open_settings(self):
        """Open plugin settings screen."""
        self.session.open(M3UConverterSettings)

    def _open_epg_importer(self):
        """Open EPG importer configuration."""
        try:
            from Plugins.Extensions.EPGImport.plugin import EPGImportConfig
            self.session.open(EPGImportConfig)
        except ImportError:
            self.session.open(MessageBox, _("EPGImport plugin not found"), MessageBox.TYPE_ERROR)

    def _purge_m3u_bouquets(self, directory="/etc/enigma2", pattern="_m3ubouquet.tv"):
        """Remove all bouquet files with dynamic EPG cleanup.

        Args:
            directory (str): Directory to search for bouquets
            pattern (str): Pattern to match bouquet files
        """
        create_bouquets_backup()
        removed_files = []

        for filename in listdir(directory):
            file_path = join(directory, filename)
            if isfile(file_path) and filename.endswith(pattern):
                try:
                    remove(file_path)
                    removed_files.append(filename)

                    bouquet_name = filename.replace('userbouquet.', '').replace('.tv', '')
                    self._remove_epg_files(bouquet_name)
                    self._remove_epg_bouquet_source(bouquet_name)

                except Exception as e:
                    logger.error("Failed to remove %s: %s", filename, str(e))

        # Clean the main bouquets.tv file
        self._clean_bouquets_file(directory, pattern)

        # Clean EPG sources if empty
        self._clean_epg_sources()

        if removed_files:
            message = _("Removed %d bouquet(s):\n%s") % (len(removed_files), "\n".join(removed_files))
        else:
            message = _("No M3UConverter bouquets found to remove.")

        self.session.open(MessageBox, message, MessageBox.TYPE_INFO, timeout=6)

    def _clean_bouquets_file(self, directory, pattern):
        """Clean the bouquets.tv file by removing references to deleted bouquets.

        Args:
            directory (str): Directory containing bouquets.tv
            pattern (str): Pattern to match bouquet files
        """
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
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Removed empty EPG sources file")
            else:
                # Write back the cleaned content
                with open(sources_file, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Updated EPG sources file")

        except Exception as e:
            logger.error("Error cleaning EPG sources: %s", str(e))

    def _remove_epg_files(self, bouquet_name):
        """Remove EPG files associated with the bouquet.

        Args:
            bouquet_name (str): Name of the bouquet
        """
        epgimport_path = "/etc/epgimport"

        # Remove channels.xml file
        channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
        if fileExists(channels_file):
            try:
                remove(channels_file)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Removed EPG channels file: %s", channels_file)
            except Exception as e:
                logger.error("Error removing EPG channels file %s: %s", channels_file, str(e))

        # Remove any .tv.epg.imported file (cache file)
        epg_imported_file = join(epgimport_path, f"{bouquet_name}.tv.epg.imported")
        if fileExists(epg_imported_file):
            try:
                remove(epg_imported_file)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Removed EPG cache file: %s", epg_imported_file)
            except Exception as e:
                logger.error("Error removing EPG cache file %s: %s", epg_imported_file, str(e))

    def _remove_epg_bouquet_source(self, bouquet_name):
        """Remove a bouquet from EPG sources.

        Args:
            bouquet_name (str): Name of the bouquet to remove

        Returns:
            bool: True if successful, False otherwise
        """
        epgimport_path = "/etc/epgimport"
        sources_path = join(epgimport_path, "ArchimedeConverter.sources.xml")

        if not fileExists(sources_path):
            return True

        try:
            with open(sources_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Remove source for this bouquet
            pattern = rf'<source type="gen_xmltv"[^>]*channels="{escape(bouquet_name)}\.channels\.xml"[^>]*>.*?</source>'
            new_content = sub(pattern, '', content, flags=DOTALL)

            # If no more bouquets, remove the entire sourcecat
            sourcecat_pattern = r'<sourcecat sourcecatname="Archimede Converter by Lululla">(.*?)</sourcecat>'
            sourcecat_match = search(sourcecat_pattern, new_content, DOTALL)

            if sourcecat_match:
                # Check if there are any sources left
                sources_content = sourcecat_match.group(1)
                if not search(r'<source type="gen_xmltv"', sources_content):
                    # No sources left, remove entire sourcecat
                    new_content = sub(sourcecat_pattern, '', new_content, flags=DOTALL)

            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Removed EPG source for bouquet: {bouquet_name}")
            return True

        except Exception as e:
            logger.error(f"Error removing EPG source: {str(e)}")
            return False

    def _select_current_item(self):
        """Handle selection of current menu item."""
        selection = self["list"].getCurrent()
        if not selection:
            return

        if selection[1] == "purge_m3u_bouquets":
            self._purge_m3u_bouquets()
            return

        self["status"].setText(_("Press RED to open file browser first"))

        self.session.open(
            UniversalConverter,
            conversion_type=selection[1],
            selected_file=None,
            auto_start=False
        )


class UniversalConverter(Screen):
    """Main universal converter screen with conversion functionality."""
    if SCREEN_WIDTH > 1280:

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

    def __init__(self, session, conversion_type=None, selected_file=None, auto_start=False):
        """Initialize universal converter screen.

        Args:
            session: Enigma2 session
            conversion_type (str): Type of conversion to perform
            selected_file (str): Path to selected file
            auto_open_browser (bool): Auto-open file browser
            auto_start (bool): Auto-start conversion
        """
        Screen.__init__(self, session)
        self.session = session
        self.conversion_type = conversion_type
        self.selected_file = selected_file
        self.auto_start = auto_start
        title_mapping = {
            "m3u_to_tv": _("M3U to Enigma2 Bouquet Conversion"),
            "tv_to_m3u": _("Enigma2 Bouquet to M3U Conversion"),
            "json_to_tv": _("JSON to Enigma2 Bouquet Conversion"),
            "json_to_m3u": _("JSON to M3U Conversion"),
            "xspf_to_m3u": _("XSPF to M3U Playlist Conversion"),
            "m3u_to_json": _("M3U to JSON Conversion")
        }
        self.setTitle(title_mapping.get(conversion_type, PLUGIN_TITLE))
        self.m3u_channels_list = []
        self.bouquet_list = []
        self.aspect_manager = AspectManager()
        self.converter = core_converter
        self.progress = None
        self.is_converting = False
        self.cancel_conversion = False
        self.epg_mapper = None
        base_path = config.plugins.m3uconverter.lastdir.value
        self.full_path = base_path
        self["list"] = MenuList([])
        self["Title"] = Label(PLUGIN_TITLE)
        self["status"] = Label(_("Ready"))
        self["key_red"] = StaticText(_("Open File"))
        self["key_green"] = StaticText("")
        self["key_yellow"] = StaticText(_("Filter"))
        self["key_blue"] = StaticText(_("Tools"))
        self.progress_source = Progress()
        self["progress_source"] = self.progress_source
        self["progress_text"] = StaticText("")
        self["progress_source"].setValue(0)
        self.initial_service = self.session.nav.getCurrentlyPlayingServiceReference()
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MediaPlayerActions", "MenuActions"], {
            "red": self._open_file_browser,
            "green": self._start_conversion_process,
            "yellow": self._toggle_channel_filter,
            "blue": self._handle_blue_button_action,
            "menu": self._open_settings,
            "ok": self._handle_ok_button,
            "cancel": self._close_screen,
            "stop": self._stop_media_player
        }, -1)
        self["status"].setText(_("Ready: Select the file from the %s you configured in settings.") % self.full_path)

        if self.conversion_type == "tv_to_m3u":
            self._initialize_tv_converter()

        if self.selected_file:
            self["status"].setText(_("File loaded: {}").format(basename(self.selected_file)))
            self.file_loaded = True
        else:
            self["status"].setText(_("Press RED to select file"))
            self.file_loaded = False

        self.onLayoutFinish.append(self._initialize_epg_mapper)

    def _open_settings(self):
        """Open plugin settings screen."""
        self.session.open(M3UConverterSettings)

    def _initialize_epg_mapper(self):
        """Initialize or reuse the EPG mapper with persistent cache.

        Returns:
            EPGServiceMapper: Initialized EPG mapper instance
        """
        if self.epg_mapper is None:
            self.epg_mapper = EPGServiceMapper(prefer_satellite=True)

            def initialization_task():
                """Background initialization task for EPG mapper."""
                try:
                    self.epg_mapper.initialize()
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("EPGServiceMapper initialized in background")
                except Exception as e:
                    logger.error(f"Background EPG initialization failed: {str(e)}")

            # Start in background thread
            import threading
            thread = threading.Thread(target=initialization_task, daemon=True)
            thread.start()

        return self.epg_mapper

    def _handle_blue_button_action(self):
        """Dynamic handling of the blue button based on current state."""
        if self.is_converting:
            self._cancel_conversion_process()
        else:
            self._show_enhanced_tools_menu()

    def _start_conversion_after_display(self):
        """Automatically start conversion after screen is displayed."""
        try:
            self.onShown.remove(self._start_conversion_after_display)
            if self.auto_start and self.selected_file:
                self.start_timer = eTimer()
                self.start_timer.callback.append(self._delayed_conversion_start)
                self.start_timer.start(2000)  # 2 second delay
        except Exception:
            pass

    def _delayed_conversion_start(self):
        """Start conversion with a slight delay."""
        try:
            self.start_timer.stop()
            self._start_conversion_process()
        except Exception as e:
            logger.error(f"Error in delayed conversion start: {str(e)}")

    def _create_manual_backup(self):
        """Create manual backup of bouquets."""
        try:
            self.converter._create_backup()
            self.session.open(MessageBox, _("Backup created successfully!"), MessageBox.TYPE_INFO, timeout=6)
        except Exception as e:
            self.session.open(MessageBox, _(f"Backup failed: {str(e)}"), MessageBox.TYPE_ERROR, timeout=6)

    def _initialize_tv_converter(self):
        """Initialize TV converter specific settings."""
        self._update_tv_path_settings()

    def _update_tv_path_settings(self):
        """Update TV path settings and check permissions."""
        try:
            if not exists("/etc/enigma2"):
                raise OSError("Bouquets path not found")

            if not access("/etc/enigma2", W_OK):
                logger.warning("Read-only bouquets path /etc/enigma2")

        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"TV path error: {str(e)}")
            self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR, timeout=6)

    def _open_file_browser(self):
        """Open file browser for file selection."""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"Opening file browser for {self.conversion_type}")

        try:
            path = ("/etc/enigma2" if self.conversion_type == "tv_to_m3u"
                    else config.plugins.m3uconverter.lastdir.value)

            pattern = r"(?i)^.*\.(tv|m3u|m3u8|json|xspf)$"

            if not path or not isdir(path):
                path = "/media/hdd" if isdir("/media/hdd") else "/tmp"

            self.session.openWithCallback(
                self._handle_file_selection,
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

    def _toggle_channel_filter(self):
        """Enable/disable filtering of non-working channels."""
        if not hasattr(self, 'selected_file') or not self.selected_file:
            self["status"].setText(_("No file loaded to filter"))
            return

        # Toggle filter setting
        config.plugins.m3uconverter.filter_dead_channels.value = \
            not config.plugins.m3uconverter.filter_dead_channels.value
        config.plugins.m3uconverter.filter_dead_channels.save()

        # Re-parse with new filter setting
        self._parse_m3u_file(self.selected_file)

        # Save and temporarily display filter status
        self._previous_status = self["status"].getText()
        status_text = _("Filter: %s") % (_("ON") if config.plugins.m3uconverter.filter_dead_channels.value else _("OFF"))
        self["status"].setText(status_text)

        # Restore previous status after delay
        self.status_timer = eTimer()
        self.status_timer.callback.append(self._restore_previous_status)
        self.status_timer.start(6000)

    def _restore_previous_status(self):
        """Restore the previous status text."""
        if hasattr(self, "_previous_status"):
            self["status"].setText(self._previous_status)

        if hasattr(self, "status_timer"):
            self.status_timer.stop()
            self.status_timer.callback.remove(self._restore_previous_status)

    def _show_enhanced_tools_menu(self):
        """Show enhanced tools menu with various utilities."""
        from Screens.ChoiceBox import ChoiceBox

        menu_items = [
            (_("🛠️ Channel Tools - Channel Management"), "header"),
            # (_("🔍 Manage Unknown Channels"), "unknown_channels"),
            (_("📊 EPG Cache Statistics"), "cache_stats"),
            (_("🔄 Reload EPG Database"), "reload_epg"),
            ("", "separator"),
            (_("⚙️ System Tools - System Utilities"), "header"),
            (_("💾 Create Backup"), "backup"),
            (_("🔄 Reload Services"), "reload"),
            (_("📋 Plugin Info"), "info"),
            (_("🧹 Clear EPG Cache"), "clear_cache")
        ]

        def tool_selection_handler(choice):
            """Handle tool selection from menu.

            Args:
                choice: Selected menu item
            """
            if choice and choice[1] not in ["header", "separator"]:
                action = choice[1]
                if action == "cache_stats":
                    self._show_cache_statistics()
                elif action == "reload_epg":
                    self._reload_epg_database()
                elif action == "clear_cache":
                    self._clear_epg_cache()
                elif action == "backup":
                    self._create_manual_backup()
                elif action == "reload":
                    self._reload_services()
                elif action == "info":
                    self._show_plugin_information()

        self.session.openWithCallback(
            tool_selection_handler,
            ChoiceBox,
            title=_("Advanced Tools Menu"),
            list=menu_items
        )

    def _show_cache_statistics(self):
        """Display EPG cache statistics."""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            try:
                stats = self.epg_mapper.get_cache_statistics()

                message_lines = [
                    _("📊 EPG CACHE STATISTICS"),
                    "",
                    _("• Cache Size: {} entries").format(stats.get('cache_size', 0)),
                    _("• Cache Hits: {}").format(stats.get('hits', 0)),
                    _("• Cache Misses: {}").format(stats.get('misses', 0)),
                    _("• Hit Rate: {}").format(stats.get('hit_rate', '0.0%')),
                    _("• Rytec Channels: {}").format(stats.get('rytec_channels', 0)),
                    _("• DVB Channels: {}").format(stats.get('loaded_dvb_channels', 0)),
                    _("• Incompatible Matches: {}").format(stats.get('incompatible_matches', 0))
                ]

                self.session.open(MessageBox, "\n".join(message_lines), MessageBox.TYPE_INFO, timeout=10)
            except Exception as e:
                logger.error(f"Error getting cache statistics: {e}")
                self.session.open(MessageBox, _("Error getting cache statistics"), MessageBox.TYPE_ERROR)
        else:
            self.session.open(MessageBox, _("EPG mapper not initialized"), MessageBox.TYPE_WARNING)

    def _reload_epg_database(self):
        """Reload EPG database."""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            try:
                # Reinitialize EPG mapper
                self.epg_mapper = EPGServiceMapper(prefer_satellite=True)
                if self.epg_mapper.initialize():
                    self.session.open(MessageBox, _("EPG database reloaded successfully!"), MessageBox.TYPE_INFO)
                else:
                    self.session.open(MessageBox, _("Failed to initialize EPG mapper"), MessageBox.TYPE_ERROR)
            except Exception as e:
                logger.error(f"Error reloading EPG database: {e}")
                self.session.open(MessageBox, _("Error reloading EPG database: {}").format(str(e)), MessageBox.TYPE_ERROR)
        else:
            self.session.open(MessageBox, _("EPG mapper not initialized"), MessageBox.TYPE_WARNING)

    def _reload_services(self):
        """Reload Enigma2 services."""
        try:
            reload_enigma2_services()
            self["status"].setText(_("Services reloaded successfully"))
            self.session.open(MessageBox, _("Services reloaded successfully"), MessageBox.TYPE_INFO, timeout=3)
        except Exception as e:
            logger.error(f"Error reloading services: {e}")
            self.session.open(MessageBox, _("Error reloading services"), MessageBox.TYPE_ERROR, timeout=3)

    def _clear_epg_cache(self):
        """Clear EPG cache."""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            try:
                cache_size = len(self.epg_mapper._match_cache)
                self.epg_mapper._match_cache.clear()
                self.epg_mapper._match_cache_hits = 0
                self.epg_mapper._match_cache_misses = 0

                self.session.open(
                    MessageBox,
                    _("EPG cache cleared! {} entries removed").format(cache_size),
                    MessageBox.TYPE_INFO
                )
            except Exception as e:
                logger.error(f"Error clearing EPG cache: {e}")
                self.session.open(MessageBox, _("Error clearing EPG cache"), MessageBox.TYPE_ERROR)
        else:
            self.session.open(MessageBox, _("EPG mapper not initialized"), MessageBox.TYPE_WARNING)

    def _format_file_size(self, size_bytes):
        """Format file size to human readable format.

        Args:
            size_bytes (int): Size in bytes

        Returns:
            str: Formatted file size
        """
        if size_bytes == 0:
            return "0 B"

        size_units = ["B", "KB", "MB", "GB"]
        unit_index = 0
        size = size_bytes

        while size >= 1024 and unit_index < len(size_units) - 1:
            size /= 1024
            unit_index += 1

        return f"{size:.2f} {size_units[unit_index]}"

    def _start_conversion_process(self):
        """Start the conversion process."""
        if self.is_converting:
            return

        if not hasattr(self, 'selected_file') or not self.selected_file:
            self.session.open(MessageBox, _("No file selected for conversion"), MessageBox.TYPE_WARNING)
            return

        # Update UI based on conversion type
        conversion_labels = {
            "m3u_to_tv": _("Converting to TV"),
            "tv_to_m3u": _("Converting to M3U"),
            "json_to_tv": _("Converting JSON to TV"),
            "json_to_m3u": _("Converting JSON to M3U"),
            "xspf_to_m3u": _("Converting XSPF to M3U"),
            "m3u_to_json": _("Converting M3U to JSON")
        }

        green_label = conversion_labels.get(self.conversion_type, _("Converting"))
        self["key_green"].setText(green_label)
        self["status"].setText(_("Conversion in progress..."))

        # Handle different conversion types
        if self.conversion_type == "m3u_to_tv":
            self._convert_m3u_to_tv()
        elif self.conversion_type == "tv_to_m3u":
            self._convert_tv_to_m3u()
        elif self.conversion_type == "json_to_tv":
            self._convert_json_to_tv()
        elif self.conversion_type == "json_to_m3u":
            self._convert_json_to_m3u()
        elif self.conversion_type == "xspf_to_m3u":
            self._convert_xspf_to_m3u()
        elif self.conversion_type == "m3u_to_json":
            self._convert_m3u_to_json()
        else:
            self.session.open(MessageBox, _("Unsupported conversion type"), MessageBox.TYPE_ERROR)

    def _handle_file_selection(self, selected_file=None):
        """Handle file selection with robust error handling.

        Args:
            selected_file (str): Path to selected file
        """
        if not selected_file or not isinstance(selected_file, (str, list, tuple)):
            self["status"].setText(_("Invalid selection"))
            return
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("=== FILE SELECTION STARTED ===")

            # Reset all states
            self.file_loaded = False
            self.m3u_channels_list = []
            self["status"].setText(_("Processing selection..."))

            # Validate input
            if not selected_file:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("No file selected")
                self["status"].setText(_("No file selected"))
                return

            # Get normalized path
            try:
                if isinstance(selected_file, (tuple, list)) and selected_file:
                    selected_path = normpath(str(selected_file[0]))
                else:
                    selected_path = normpath(str(selected_file))

                if not exists(selected_path):
                    raise IOError(_("File not found"))
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Path error: {str(e)}")
                self._show_error_message(str(e))
                return

            # DEBUG: Before processing
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Processing file: {selected_path}")

            # Process file based on conversion type
            path = selected_file[0] if isinstance(selected_file, (list, tuple)) else selected_file
            self.selected_file = normpath(str(path))

            try:
                if self.conversion_type == "m3u_to_tv":
                    self._parse_m3u_file(selected_path)
                elif self.conversion_type == "tv_to_m3u":
                    self._parse_tv_file(selected_path)
                elif self.conversion_type == "m3u_to_json":
                    self._parse_m3u_file(selected_path)
                elif self.conversion_type == "json_to_m3u":
                    self._parse_json_file(selected_path)
                elif self.conversion_type == "json_to_tv":
                    self._parse_json_file(selected_path)

                # Update state
                self.file_loaded = True
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"File loaded successfully, channels: {len(self.m3u_channels_list)}")
            except Exception as e:
                logger.error(f"Processing failed: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"File selection failed: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            self._show_error_message(str(e))
        finally:
            logger.debug("=== FILE SELECTION COMPLETE ===")

    def update_main_bouquet(self, groups):
        """Update the main bouquet file with generated group bouquets.
        Args:
            groups: List of group names to add to bouquet

        Returns:
            bool: True if successful, False otherwise
        """
        main_file = "/etc/enigma2/bouquets.tv"
        # Read existing content
        existing_lines = []
        if exists(main_file):
            try:
                with open(main_file, "r", encoding="utf-8") as f:
                    existing_lines = f.readlines()
            except Exception as e:
                logger.error(f"Error reading bouquets.tv: {str(e)}")
                return False

        # Create new lines to add
        new_lines = []
        for group in groups:
            safe_name = self.get_safe_filename(group)
            bouquet_path = "userbouquet." + safe_name + ".tv"
            line_to_add = '#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "' + bouquet_path + '" ORDER BY bouquet\n'

            # Check if line already exists
            if not any(line_to_add in line for line in existing_lines):
                new_lines.append(line_to_add)

        if not new_lines:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("No new bouquets to add")
            return True

        # Add new lines in correct position
        if config.plugins.m3uconverter.bouquet_position.value == "top":
            final_content = new_lines + existing_lines
        else:
            final_content = existing_lines + new_lines

        # Write file
        try:
            with open(main_file, "w", encoding="utf-8") as f:
                f.writelines(final_content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Updated bouquets.tv with {len(new_lines)} new bouquets")
            return True
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error writing bouquets.tv: {str(e)}")
            return False

    def _update_ui_success(self, channel_count):
        """Update UI only if necessary."""
        if not hasattr(self, '_last_channel_count') or self._last_channel_count != channel_count:
            self._last_channel_count = channel_count

            conversion_ready_texts = {
                "m3u_to_tv": _("Convert to TV"),
                "tv_to_m3u": _("Convert to M3U"),
                "json_to_tv": _("Convert JSON to TV"),
                "json_to_m3u": _("Convert JSON to M3U"),
                "xspf_to_m3u": _("Convert XSPF to M3U"),
                "m3u_to_json": _("Convert M3U to JSON")
            }

            ready_text = conversion_ready_texts.get(self.conversion_type, _("Ready to convert"))
            self["key_green"].setText(ready_text)
            self["status"].setText(_("Loaded %d channels. Press Green to convert.") % channel_count)

    def _process_url(self, url):
        """Process URLs correctly - handle already encoded URLs.

        Args:
            url (str): URL to process

        Returns:
            str: Processed URL
        """
        if not url:
            return url

        # Check if URL is already encoded
        if '%3a' in url or '%3A' in url:

            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"URL already encoded: {url}")
            return url

        # Apply encoding if not already encoded
        url = url.replace(":", "%3a")

        if config.plugins.m3uconverter.hls_convert.value:
            if any(url.lower().endswith(ext) for ext in ('.m3u8', '.stream')):
                url = f"hls://{url}"

        return url

    def write_group_bouquet(self, group, channels):
        """Write a bouquet file for a single group safely and efficiently.

        Args:
            group (str): Group name
            channels (list): List of channel dictionaries

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            safe_name = self.get_safe_filename(group)
            bouquet_dir = "/etc/enigma2"
            filename = join(bouquet_dir, "userbouquet." + safe_name + ".tv")
            temp_file = filename + ".tmp"

            if not exists(bouquet_dir):
                makedirs(bouquet_dir, exist_ok=True)

            name_bouquet = clean_group_name(self.remove_suffixes(group))

            with open(temp_file, "w", encoding="utf-8", buffering=65536) as f:
                f.write(f"#NAME {name_bouquet}\n")
                f.write("#SERVICE 1:64:0:0:0:0:0:0:0:0::--- | Archimede Converter | ---\n")
                f.write("#DESCRIPTION --- | Archimede Converter | ---\n")

                for ch in channels:
                    if not ch.get('url') or len(ch['url']) < 10:
                        continue

                    # Use correct service reference
                    service_ref = ch.get('sref') or ch.get('url', '')
                    f.write(f"#SERVICE {service_ref}\n")

                    # Clean name for description
                    desc = ch.get('name', 'Unknown Channel')
                    desc = ''.join(c for c in desc if c.isprintable() or c.isspace())
                    desc = transliterate_text(desc)
                    f.write(f"#DESCRIPTION {desc}\n")

            # Replace temporary file with final file
            if exists(filename):
                remove(filename)

            replace(temp_file, filename)

            # Set correct permissions
            chmod(filename, 0o644)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Successfully wrote bouquet: {safe_name} with {len(channels)} channels")

            return True

        except Exception as e:
            if exists(temp_file):
                try:
                    remove(temp_file)
                except Exception:
                    pass

            logger.error(f"Failed to write bouquet {group}: {str(e)}")
            return False

    def remove_suffixes(self, name):
        """Remove all known suffixes from the name for display purposes.

        Args:
            name (str): Original name with suffixes

        Returns:
            str: Cleaned name without suffixes
        """
        suffixes = ['_m3ubouquet', '_bouquet', '_iptv', '_tv']

        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break

        return name

    def get_safe_filename(self, name):
        """Generate a secure file name for bouquets with duplicate suffixes.

        Args:
            name (str): Original group name

        Returns:
            str: Safe filename
        """
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
        """Generate a unique file name for export.

        Returns:
            str: Output filename with timestamp
        """
        timestamp = strftime("%Y%m%d_%H%M%S")
        return f"{ARCHIMEDE_CONVERTER_PATH}/archimede_export_{timestamp}.m3u"

    def _is_valid_text(self, text):
        """Check if text is valid - more permissive for URLs.

        Args:
            text: Text to validate

        Returns:
            bool: True if text is valid
        """
        if not text or not isinstance(text, str):
            return False

        if len(text) > 100:
            return True

        text_str = str(text)

        # Printable characters check - more permissive
        printable_count = sum(1 for c in text_str if c.isprintable() or c.isspace())
        if printable_count / len(text_str) < 0.5:  # Only 50% printable
            return False

        return True

    def filter_binary_data(self, channels):
        """Filter out channels with binary data in names or URLs.

        Args:
            channels (list): List of channel dictionaries

        Returns:
            list: Filtered list of channels
        """
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
        """Handle extremely large M3U files with sampling.

        Args:
            filename (str): Path to large file

        Returns:
            list: Sampled channel entries
        """
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

    def _parse_m3u_file(self, filename=None):
        """Parse M3U file with improved attribute handling.

        Args:
            filename (str): Path to M3U file
        """
        try:
            file_to_parse = filename or self.selected_file
            if not file_to_parse:
                raise ValueError(_("No file specified"))

            file_size = getsize(file_to_parse)

            # Use appropriate parsing method based on file size
            if file_size > 10 * 1024 * 1024:  # > 10MB
                self.m3u_channels_list = self.handle_very_large_file(file_to_parse)
            elif file_size > 1 * 1024 * 1024:  # > 1MB
                self.m3u_channels_list = self._parse_m3u_incremental(file_to_parse)
            else:
                with open(file_to_parse, 'r', encoding='utf-8', errors='replace') as f:
                    data = f.read()
                self.m3u_channels_list = self._parse_m3u_content(data)

            # Filter and process channels with proper attribute mapping
            filtered_channels = []
            for channel in self.m3u_channels_list:
                if channel.get('uri'):
                    # Map attributes to consistent names
                    filtered_channels.append({
                        'name': channel.get('title', ''),
                        'url': self._process_url(channel['uri']),
                        'group': channel.get('group-title', ''),
                        'tvg_id': channel.get('tvg-id', ''),
                        'tvg_name': channel.get('tvg-name', ''),
                        'logo': channel.get('tvg-logo', ''),
                        'duration': channel.get('length', '-1'),
                        'user_agent': channel.get('user-agent', ''),
                        'language': channel.get('tvg-language', '')
                    })

            self.m3u_channels_list = filtered_channels

            # Update UI
            display_list = []
            for idx, channel in enumerate(self.m3u_channels_list[:100]):  # Show only first 100
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', 'Default')
                group = clean_group_name(group)
                display_list.append(f"{idx + 1:03d}. {group + ' - ' if group else ''}{name}")

            self["list"].setList(display_list)
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_channels_list))

        except Exception as e:
            logger.error(f"Error parsing M3U: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            self.session.open(
                MessageBox,
                _("Error parsing file. File may be too large or corrupt."),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _parse_m3u_content(self, data):
        """Advanced parser for M3U content with support for extended attributes.

        Args:
            data (str): M3U file content

        Returns:
            list: Parsed channel entries
        """
        entries = []
        current_params = {}
        attributes = {}
        lines_processed = 0
        lines = data.split('\n')
        for line in lines:
            lines_processed += 1
            line = line.strip()
            if not line:
                continue

            if lines_processed % 100 == 0:
                from enigma import eTimer
                eTimer().start(10, True)

            if line.startswith('#EXTM3U'):
                continue

            elif line.startswith('#EXTINF:'):
                current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                extinf_content = line[8:].strip()

                if ',' in extinf_content:
                    last_comma_index = extinf_content.rfind(',')
                    attributes_part = extinf_content[:last_comma_index].strip()
                    title_part = extinf_content[last_comma_index + 1:].strip()
                    current_params['title'] = title_part
                    attr_matches = findall(r'(\S+?)="([^"]*)"', attributes_part)
                    for key, value in attr_matches:
                        attributes[key.lower()] = value

                    duration_match = search(r'^(-?\d+)', attributes_part)
                    if duration_match:
                        attributes['length'] = duration_match.group(1)

                    current_params.update(attributes)

                else:
                    current_params['title'] = extinf_content

                common_attributes = ['tvg-id', 'tvg-name', 'tvg-logo', 'group-title', 'tvg-language']
                for attr in common_attributes:
                    if attr in attributes:
                        current_params[attr] = attributes[attr]

            elif line.startswith('#EXTGRP:'):
                current_params['group-title'] = str(line[8:].strip())

            elif line.startswith('#EXTVLCOPT:'):
                opt_line = line[11:].strip()
                if '=' in opt_line:
                    key, value = opt_line.split('=', 1)
                    key = key.lower().strip()
                    if key == 'http-user-agent':
                        current_params['user-agent'] = str(value.strip())

            elif line.startswith('#'):
                continue

            elif not line.startswith('#'):
                if current_params and line:
                    current_params['uri'] = str(line.strip())
                    if current_params.get('title'):
                        for key in list(current_params.keys()):
                            if isinstance(current_params[key], list):
                                current_params[key] = str(current_params[key][0]) if current_params[key] else ''

                        entries.append(current_params.copy())
                    current_params = {}
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Parsing completed: {len(entries)} channels found")
        return entries

    def _parse_m3u_incremental(self, filename, chunk_size=32768):
        """Parse M3U file incrementally with attribute support.

        Args:
            filename (str): Path to M3U file
            chunk_size (int): Size of chunks to read

        Returns:
            list: Parsed channel entries
        """
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

    def _parse_tv_file(self, filename=None):
        """Parse TV bouquet file.

        Args:
            filename (str): Path to TV bouquet file
        """
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

            self.m3u_channels_list = channels
            self["list"].setList([c[0] for c in channels])
            self.file_loaded = True
            self._update_ui_success(len(self.m3u_channels_list))
        except Exception as e:
            logger.error(f"Error parsing BOUQUET: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            raise

    def _parse_json_file(self, filename=None):
        """Parse JSON file containing channel information.

        Args:
            filename (str): Path to JSON file
        """
        file_to_parse = filename or self.selected_file
        try:
            with open(file_to_parse, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.m3u_channels_list = []
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
                    self.m3u_channels_list.append({
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
            for channel in self.m3u_channels_list:
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', '')
                display_list.append(f"{group + ' - ' if group else ''}{name}")

            # Update the list immediately
            self["list"].setList(display_list)
            self.file_loaded = True

            self._update_ui_success(len(self.m3u_channels_list))

            # Log results for debugging
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Found {len(self.m3u_channels_list)} channels in JSON file")
            if len(self.m3u_channels_list) > 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Sample channel: {self.m3u_channels_list[0]}")

        except Exception as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            self.session.open(
                MessageBox,
                _("Error parsing JSON file. Please check the format.\n\nError: %s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _real_conversion_task(self, m3u_path=None, progress_callback=None):
        """Optimized conversion with batch processing"""
        if not m3u_path and not self.selected_file:
            logger.error("No file specified for conversion")
            return (False, "No file specified")

        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper.reset_caches()
            # Pre-carica le cache per performance
            self.epg_mapper.optimize_matching()

        try:
            # DEFINE file_to_parse correctly
            file_to_parse = m3u_path or self.selected_file
            if not file_to_parse:
                logger.error("No file specified for conversion")
                return (False, "No file specified")

            logger.info(f"Starting conversion for: {file_to_parse}")

            # Extract EPG URL from M3U file
            epg_url = None
            try:
                epg_url = self.epg_mapper.extract_epg_url_from_m3u(file_to_parse)
                logger.info(f"Extracted EPG URL: {epg_url}")
            except Exception as e:
                logger.warning(f"Error extracting EPG URL: {str(e)}")
                epg_url = None

            # Parse file if not already parsed
            if file_to_parse and not self.m3u_channels_list:
                logger.info(f"Parsing file: {file_to_parse}")
                with open(file_to_parse, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                self.m3u_channels_list = self._parse_m3u_content(content)

                # Normalize fields
                normalized_list = []
                for ch in self.m3u_channels_list:
                    normalized_list.append({
                        'name': ch.get('title', ''),
                        'url': ch.get('uri', ''),
                        'group': ch.get('group-title', ''),
                        'tvg_id': ch.get('tvg-id', ''),
                        'tvg_name': ch.get('tvg-name', ''),
                        'logo': ch.get('tvg-logo', ''),
                        'user_agent': ch.get('user_agent', ''),
                        'program_id': ch.get('program-id', '')
                    })
                self.m3u_channels_list = normalized_list

            total_channels = len(self.m3u_channels_list)
            if total_channels == 0:
                logger.error("No valid channels found after parsing")
                return (False, "No valid channels found")

            groups = {}
            epg_data = []
            stats = {
                'total_channels': total_channels,
                'rytec_matches': 0,
                'dvb_matches': 0,
                'fallback_matches': 0,
                'iptv_fallback': 0
            }

            # DEBUG: Log what we're working with
            logger.debug("Starting conversion with %d channels", total_channels)
            for i, ch in enumerate(self.m3u_channels_list[:5]):  # First 5 channels
                logger.debug("Channel %d: %s -> %s", i + 1, ch.get('name'), ch.get('url'))

            # Phase 1: Process each channel
            for idx, channel in enumerate(self.m3u_channels_list):
                if self.cancel_conversion:
                    return (False, "Conversion cancelled")

                if not channel.get('url'):
                    continue

                # Get channel info
                name = channel.get('name', 'Unknown')
                url = channel.get('url', '')
                tvg_id = channel.get('tvg_id', '')
                original_name = name  # Preserve original name
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Processing channel {idx}: {name}")

                # Clean name for matching (without variants for better matching)
                clean_name = self.epg_mapper.clean_channel_name(name, preserve_variants=False)

                # Find best service reference with DEBUG
                service_ref, match_type = self.epg_mapper.find_best_service_match(clean_name, tvg_id, original_name)
                epg_sref = None
                if service_ref:
                    epg_sref = service_ref
                    stats['rytec_matches'] += 1
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"EPG MATCH SUCCESS: {name} -> {service_ref} ({match_type})")
                else:

                    if url and url != 'iptv_fallback':
                        import hashlib
                        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
                        service_id = int(url_hash, 16) % 65536
                        epg_sref = f"4097:0:1:{service_id}:0:0:0:0:0:0"
                    else:
                        import hashlib
                        name_hash = hashlib.md5(name.encode('utf-8')).hexdigest()[:8]
                        service_id = int(name_hash, 16) % 65536
                        epg_sref = f"4097:0:1:{service_id}:0:0:0:0:0:0"

                    stats['iptv_fallback'] += 1

                if service_ref:
                    channel['sref'] = self.epg_mapper.generate_hybrid_sref(service_ref, url)
                else:
                    channel['sref'] = self.epg_mapper.generate_service_reference(url)

                # CREATE EPG ENTRY FOR ALL CHANNELS - PRESERVE VARIANTS
                epg_entry = {
                    'tvg_id': tvg_id or name,
                    'sref': epg_sref,
                    'name': name,
                    'url': url,
                    'original_name': original_name,
                    'match_type': match_type or 'iptv_fallback'
                }
                epg_data.append(epg_entry)

                # RESPECT SELECTED MODE
                if config.plugins.m3uconverter.bouquet_mode.value == "single":
                    group = "All Channels"
                else:
                    group = clean_group_name(channel.get('group', 'Default'))

                # Group channels
                groups.setdefault(group, []).append(channel)

                # Update progress
                if idx % 50 == 0:
                    progress = (idx + 1) / total_channels * 100
                    self.update_progress(idx + 1, _("Processing: %s (%d%%)") % (name, progress))

            # DEBUG: Log created groups
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("Groups created: %s", list(groups.keys()))
                logger.debug("Bouquet mode: %s", config.plugins.m3uconverter.bouquet_mode.value)
                logger.debug("Number of groups: %d", len(groups))

                # Mini pause for UI responsiveness (5ms)
                import time
                time.sleep(0.005)

            # Phase 2: Write bouquets
            bouquet_names = []

            if config.plugins.m3uconverter.bouquet_mode.value == "single":
                # Create a single bouquet with all channels
                all_channels = []
                for group_channels in groups.values():
                    all_channels.extend(group_channels)

                bouquet_name = self.get_safe_filename(basename(file_to_parse).split('.')[0])
                if self.write_group_bouquet(bouquet_name, all_channels):
                    bouquet_names.append(bouquet_name)

                    # DEBUG: Verify bouquet was written
                    bouquet_file = join("/etc/enigma2", f"userbouquet.{bouquet_name}.tv")
                    if fileExists(bouquet_file):
                        file_size = getsize(bouquet_file)
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info("Bouquet %s created successfully (%d bytes)", bouquet_name, file_size)
                    else:
                        logger.error("Bouquet file not created: %s", bouquet_name)
            else:
                # Create separate bouquets for each group
                for group_name, group_channels in groups.items():
                    safe_name = self.get_safe_filename(group_name)
                    if self.write_group_bouquet(safe_name, group_channels):
                        bouquet_names.append(safe_name)

                        # DEBUG: Verify bouquet was written
                        bouquet_file = join("/etc/enigma2", f"userbouquet.{safe_name}.tv")
                        if fileExists(bouquet_file):
                            file_size = getsize(bouquet_file)
                            logger.info("Bouquet %s created successfully (%d bytes)", safe_name, file_size)
                        else:
                            logger.error("Bouquet file not created: %s", safe_name)

            # Phase 3: Update main bouquet
            if bouquet_names:
                self.update_main_bouquet(bouquet_names)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Main bouquet updated with %d bouquets", len(bouquet_names))
            else:
                logger.error("No bouquets were created!")

            # Phase 4: EPG generation (if enabled)
            if config.plugins.m3uconverter.epg_enabled.value and epg_data:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"EPG enabled, preparing to generate files for {len(epg_data)} channels")

                # DEFINE bouquet_name_for_epg based on mode
                if config.plugins.m3uconverter.bouquet_mode.value == "single":
                    bouquet_name_for_epg = bouquet_names[0] if bouquet_names else "default_bouquet"
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Single bouquet mode, using: {bouquet_name_for_epg}")
                else:
                    bouquet_name_for_epg = "all_groups"
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Multi bouquet mode, using: {bouquet_name_for_epg}")

                # GENERATE EPG FILES
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Calling generate_epg_files for {bouquet_name_for_epg}")
                epg_success = self.epg_mapper.generate_epg_files(bouquet_name_for_epg, epg_data, epg_url)

                if epg_success:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"EPG files generated successfully for {bouquet_name_for_epg}")

                    # # VERIFY results
                    # self.epg_mapper.verify_epg_generation(bouquet_name_for_epg, len(epg_data))

                    # # VERIFY that files were created
                    # self.epg_mapper.verify_epg_files(bouquet_name_for_epg)
                else:
                    logger.warning(f"Failed to generate EPG files for {bouquet_name_for_epg}")

                    # Additional debug
                    self.epg_mapper._debug_epg_failure(bouquet_name_for_epg, epg_data)

            # Log final statistics
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("=== CONVERSION STATISTICS ===")
                logger.info(f"Total channels processed: {total_channels}")
                logger.info(f"Channels with EPG match: {len(epg_data)}")
                logger.info(f"EPG match rate: {(len(epg_data) / total_channels * 100):.1f}%")

            # FINAL RETURN with statistics
            cache_stats = self.epg_mapper.get_cache_statistics()
            return (True, total_channels, len(epg_data), cache_stats)

        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return (False, str(e))

    def _convert_m3u_to_tv(self):
        """Convert M3U to TV bouquet format."""
        def conversion_task():
            try:
                return self.converter.safe_conversion(self._real_conversion_task, self.selected_file, None)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_tv_to_m3u(self):
        """Convert TV bouquet to M3U format."""
        def _real_tv_to_m3u_conversion():
            try:
                output_file = self._get_output_filename()
                total_items = len(self.m3u_channels_list)

                with open(output_file, 'w', encoding='utf-8') as f:
                    if self.cancel_conversion:
                        return (False, "Conversion cancelled")

                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {CURRENT_VERSION}\n')

                    for idx, (name, url) in enumerate(self.m3u_channels_list):
                        f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                        f.write(f'{url}\n')

                        if idx % 10 == 0:
                            progress = (idx + 1) / total_items * 100
                            self.update_progress(
                                idx + 1,
                                _("Exporting: %s (%d%%)") % (name, int(progress))
                            )

                return (True, output_file, total_items)

            except IOError as e:
                return (False, _("File write error: %s") % str(e))
            except Exception as e:
                return (False, _("tv_to_m3u Conversion error: %s") % str(e))

        def conversion_task():
            try:
                return self.converter.safe_conversion(_real_tv_to_m3u_conversion)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"TV-to-M3U conversion failed: {str(e)}")
                return (False, str(e))

        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))
        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_json_to_m3u(self):
        """Convert JSON to M3U format."""
        def _json_to_m3u_conversion():
            try:
                if not self.m3u_channels_list:
                    self._parse_json_file(self.selected_file)

                if not self.m3u_channels_list:
                    return (False, "No valid channels found in JSON file")

                total_channels = len(self.m3u_channels_list)
                base_name = basename(self.selected_file).split('.')[0]
                output_dir = dirname(self.selected_file)
                output_file = join(output_dir, f"{base_name}.m3u")

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {CURRENT_VERSION}\n')

                    for idx, channel in enumerate(self.m3u_channels_list):
                        if not channel.get('url'):
                            continue

                        if self.cancel_conversion:
                            return (False, "Conversion cancelled")

                        # Update progress
                        if idx % 10 == 0:
                            progress = (idx + 1) / total_channels * 100
                            self.update_progress(
                                idx + 1,
                                _("Exporting: %s (%d%%)") % (channel.get('name', 'Unknown'), progress)
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

                return (True, output_file, total_channels)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Error converting JSON to M3U: {str(e)}")
                return (False, str(e))

        # Start conversion in thread
        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))

        def conversion_task():
            try:
                return self.converter.safe_conversion(_json_to_m3u_conversion)
            except Exception as e:
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_m3u_to_json(self):
        """Convert M3U playlist to JSON format."""
        def _m3u_to_json_conversion():
            try:
                # Parse the M3U file if not already parsed
                if not self.m3u_channels_list:
                    with open(self.selected_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    self.m3u_channels_list = self._parse_m3u_content(content)

                if not self.m3u_channels_list:
                    return (False, "No valid channels found in M3U file")

                # Create JSON structure
                json_data = {"playlist": []}

                for idx, channel in enumerate(self.m3u_channels_list):
                    # Update progress
                    progress = (idx + 1) / len(self.m3u_channels_list) * 100
                    self.update_progress(
                        idx + 1,
                        _("Converting: %s (%d%%)") % (channel.get('title', 'Unknown'), progress)
                    )

                    # Copy all attributes found in parsing
                    channel_data = {}
                    for key, value in channel.items():
                        if self.cancel_conversion:
                            return (False, "Conversion cancelled")
                        channel_data[key] = value

                    json_data["playlist"].append(channel_data)

                # Generate output filename
                base_name = basename(self.selected_file).split('.')[0]
                output_dir = dirname(self.selected_file)
                output_file = join(output_dir, f"{base_name}.json")

                # Write JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=4, ensure_ascii=False)

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
                return self.converter.safe_conversion(_m3u_to_json_conversion)
            except Exception as e:
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_json_to_tv(self):
        """Convert JSON to TV bouquet format."""
        if not self.m3u_channels_list:
            self._parse_json_file(self.selected_file)

        if not self.m3u_channels_list:
            self.session.open(MessageBox, _("No valid channels found in JSON file"), MessageBox.TYPE_ERROR)
            return

        def _json_tv_conversion():
            try:
                result = self.converter.safe_conversion(self._real_conversion_task)
                if result[0]:  # Success
                    cache_stats = self.epg_mapper.get_cache_statistics()
                    stats_data = {
                        'total_channels': result[1],
                        'rytec_matches': result[2],
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
        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))

        threads.deferToThread(_json_tv_conversion).addBoth(self.conversion_finished)

    def _convert_xspf_to_m3u(self):
        """Convert XSPF to M3U format."""
        def _xspf_conversion():
            try:
                tree = ET.parse(self.selected_file)
                root = tree.getroot()
                output_file = self._get_output_filename()
                ns = {'ns': 'http://xspf.org/ns/0/'}

                tracks = root.findall('.//ns:track', ns)
                total_items = len(tracks)

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {CURRENT_VERSION}\n')

                    for track in root.findall('.//ns:track', ns):
                        title = track.find('ns:title', ns)
                        location = track.find('ns:location', ns)

                        if title is not None and location is not None:
                            if self.cancel_conversion:
                                return (False, "Conversion cancelled")

                            f.write(f'#EXTINF:-1,{title.text}\n')
                            f.write(f'{location.text}\n')

                            if track % 10 == 0:
                                progress = (track + 1) / total_items * 100
                                self.update_progress(
                                    track + 1,
                                    _("Exporting: %s (%d%%)") % (title, int(progress))
                                )

                track_count = len(tracks)

                return (True, output_file, track_count)
            except ET.ParseError:
                return (False, _("Invalid XSPF file"))
            except Exception as e:
                return (False, _("XSPF conversion error: %s") % str(e))

        def conversion_task():
            try:
                return self.converter.safe_conversion(_xspf_conversion)
            except Exception as e:
                return (False, str(e))

        # Start conversion
        self.is_converting = True
        self.cancel_conversion = False
        self["key_green"].setText(_("Converting"))
        self["key_blue"].setText(_("Cancel"))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def conversion_finished(self, result):
        """Handle conversion completion with proper stats display.

        Args:
            result: Conversion result tuple
        """
        self["progress_source"].setValue(0)

        try:
            self.is_converting = False
            self.cancel_conversion = False
            self["key_green"].setText(_("Convert"))
            self["key_blue"].setText(_("Tools"))

            if not isinstance(result, tuple):
                logger.error(f"Invalid result format: {result}")
                self.session.open(MessageBox, _("Conversion completed with unknown result"), MessageBox.TYPE_INFO)
                return

            success = result[0]
            data = result[1:] if len(result) > 1 else []

            if success:
                if self.conversion_type in ["m3u_to_tv", "json_to_tv"]:
                    if len(result) >= 4:
                        cache_stats = result[3]

                        stats_data = {
                            'total_channels': result[1],
                            'rytec_matches': result[2],
                            'cache_stats': cache_stats,
                            'conversion_type': self.conversion_type,
                            'timestamp': strftime("%Y-%m-%d %H:%M:%S"),
                            'status': 'completed'
                        }

                    else:
                        stats_data = self._prepare_stats_data(data, self.conversion_type)
                else:
                    stats_data = self._prepare_stats_data(data, self.conversion_type)

                self.show_conversion_stats(self.conversion_type, stats_data)
                self._reload_services_after_delay()
            else:
                error_msg = data[0] if data and len(data) > 0 else _("Unknown error")
                self.session.open(
                    MessageBox,
                    _("Conversion failed: {}").format(error_msg),
                    MessageBox.TYPE_ERROR,
                    timeout=6
                )

            if config.plugins.m3uconverter.enable_debug.value:
                self.print_performance_stats()

            self["status"].setText(_("Conversion completed"))
            self["progress_text"].setText("")

        except Exception as e:
            logger.error(f"Error in conversion_finished: {str(e)}")
            self.session.open(MessageBox, _("Error processing conversion result"), MessageBox.TYPE_ERROR)

    def _reload_services_after_delay(self, delay=2000):
        """Reload services after a delay.

        Args:
            delay (int): Delay in milliseconds
        """
        try:
            self.reload_timer = eTimer()
            self.reload_timer.callback.append(self._perform_service_reload)
            self.reload_timer.start(delay)
        except Exception as e:
            logger.error(f"Error scheduling service reload: {str(e)}")

    def _perform_service_reload(self):
        """Actually perform service reload."""
        try:
            if hasattr(self, 'reload_timer'):
                self.reload_timer.stop()

            logger.info("Auto-reloading services after conversion...")
            success = reload_enigma2_services()

            if success:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Services reloaded successfully")
                self._verify_bouquets_loaded()
            else:
                logger.warning("Service reload may have failed")

        except Exception as e:
            logger.error(f"Error in service reload: {str(e)}")

    def _verify_bouquets_loaded(self):
        """Verify that bouquets were loaded correctly."""
        try:
            bouquets_file = "/etc/enigma2/bouquets.tv"
            if fileExists(bouquets_file):
                with open(bouquets_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    bouquet_count = content.count('FROM BOUQUET "userbouquet.')
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Found {bouquet_count} bouquets in main file")
        except Exception as e:
            logger.error(f"Error verifying bouquets: {str(e)}")

    def _check_conversion_status(self):
        """Check if the conversion was canceled successfully."""
        self.cancel_timer.stop()
        if self.is_converting:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.warning("Conversion not cancelled properly, forcing termination")
            self._conversion_cancelled()

    def _cancel_conversion_process(self):
        """Cancel the ongoing conversion."""
        if self.is_converting:
            self.cancel_conversion = True
            self["key_blue"].setText(_("Cancelling..."))
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Conversion cancellation requested")

            self.cancel_timer = eTimer()
            self.cancel_timer.callback.append(self._check_conversion_status)
            self.cancel_timer.start(1000)

    def _conversion_cancelled(self):
        """Handle conversion cancellation."""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))
        self.session.open(MessageBox, _("Conversion cancelled"), MessageBox.TYPE_INFO, timeout=6)

    def _conversion_error(self, error_msg):
        """Handle conversion error.

        Args:
            error_msg (str): Error message
        """
        self.is_converting = False
        self.cancel_conversion = False
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))
        self.session.open(MessageBox, _("Conversion error: %s") % error_msg, MessageBox.TYPE_ERROR, timeout=6)

    def update_progress(self, value, text):
        """Update the progress bar safely.

        Args:
            value (int): Progress value
            text (str): Progress text
        """
        try:
            from twisted.internet import reactor
            reactor.callFromThread(self._update_progress_ui, value, text)
        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}")

    def _update_progress_ui(self, value, text):
        """Update progress UI.

        Args:
            value (int): Progress value
            text (str): Progress text
        """
        try:
            total_items = len(self.m3u_channels_list) if self.m3u_channels_list else 100
            self.progress_source.setRange(total_items)
            self.progress_source.setValue(value)
            self["progress_text"].setText(str(text))
        except Exception as e:
            logger.error(f"Error in UI progress update: {str(e)}")

    def _handle_ok_button(self):
        """Handle OK button press for channel playback."""
        index = self["list"].getSelectedIndex()
        if index < 0 or index >= len(self.m3u_channels_list):
            self["status"].setText(_("No file selected"))
            return

        item = self.m3u_channels_list[index]

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
        """Start media player with specified channel.

        Args:
            name (str): Channel name
            url (str): Stream URL
        """
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

    def _stop_media_player(self):
        """Stop media player and restore original service."""
        self.session.nav.stopService()
        if hasattr(self, 'initial_service') and self.initial_service:
            self.session.nav.playService(self.initial_service)
        self["status"].setText(_("Ready"))

    def on_movieplayer_exit(self, result=None):
        self.session.nav.stopService()
        if hasattr(self, 'initial_service') and self.initial_service:
            self.session.nav.playService(self.initial_service)
        if hasattr(self, 'aspect_manager'):
            self.aspect_manager.restore_aspect()
        self.close()

    def _close_screen(self, result=None):
        try:
            if hasattr(self, 'initial_service') and self.initial_service:
                self.session.nav.playService(self.initial_service)

            if hasattr(self, 'aspect_manager'):
                self.aspect_manager.restore_aspect()

            self.close()
        except Exception as e:
            logger.error(f"Error during close: {str(e)}")
            self.close()

    def _prepare_stats_data(self, data, conversion_type):
        """Prepare data for statistics based on conversion type."""
        stats_data = {
            'conversion_type': conversion_type,
            'status': 'completed',
            'timestamp': strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            if conversion_type in ["m3u_to_tv", "json_to_tv"]:
                # Format correct: (success, total_channels, rytec_matches, cache_stats)
                if len(data) >= 3:
                    stats_data.update({
                        'total_channels': data[1] if isinstance(data[1], int) else 0,  # Indice 1
                        'rytec_matches': data[2] if isinstance(data[2], int) else 0,    # Indice 2
                        'cache_stats': data[3] if len(data) > 3 and isinstance(data[3], dict) else {}
                    })

                    # Calcola la percentuale di copertura EPG
                    total = stats_data['total_channels']
                    epg = stats_data['rytec_matches']
                    if total > 0:
                        stats_data['epg_coverage'] = (epg / total * 100)
                    else:
                        stats_data['epg_coverage'] = 0

            elif conversion_type in ["tv_to_m3u", "json_to_m3u", "xspf_to_m3u"]:
                # Formato: (success, output_file, total_channels)
                if len(data) >= 2:
                    output_file = data[0] if isinstance(data[0], str) else ""
                    total_channels = data[1] if isinstance(data[1], int) else 0

                    stats_data.update({
                        'total_channels': total_channels,
                        'output_file': basename(output_file) if output_file else "",
                        'file_size': getsize(output_file) if output_file and exists(output_file) else 0
                    })

            elif conversion_type == "m3u_to_json":
                # Format: (success, output_file, total_channels)
                if len(data) >= 2:
                    output_file = data[0] if isinstance(data[0], str) else ""
                    total_channels = data[1] if isinstance(data[1], int) else 0

                    stats_data.update({
                        'total_channels': total_channels,
                        'output_file': basename(output_file) if output_file else "",
                        'file_size': getsize(output_file) if output_file and exists(output_file) else 0,
                        'json_structure': 'playlist'
                    })

            stats_data['timestamp'] = strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            logger.error(f"Error preparing stats data: {str(e)}")
            stats_data['error'] = str(e)

        return stats_data

    def show_conversion_stats(self, conversion_type, stats_data):
        """Show conversion statistics in a popup for all conversion types."""
        stats_message = [_("🎯 CONVERSION COMPLETE"), ""]
        try:
            if conversion_type in ["m3u_to_tv", "json_to_tv"]:
                total_channels = stats_data.get('total_channels', 0)
                rytec_matches = stats_data.get('rytec_matches', 0)
                epg_coverage = stats_data.get('epg_coverage', 0)  # Ora questo avrà un valore
                cache_stats = stats_data.get('cache_stats', {})
                stats_message.extend([
                    _("📊 Total channels processed: {}").format(total_channels),
                    _("📡 EPG mapped channels: {}").format(rytec_matches),
                    _("📈 EPG coverage: {:.1f}%").format(epg_coverage)
                ])

                if cache_stats:
                    hit_rate = cache_stats.get('hit_rate', '0.0%')
                    cache_size = cache_stats.get('cache_size', 0)
                    total_requests = cache_stats.get('total_requests', 0)
                    rytec_channels = cache_stats.get('rytec_channels', 0)
                    dvb_channels = cache_stats.get('loaded_dvb_channels', 0)

                    stats_message.extend([
                        _("💾 Cache efficiency: {}").format(hit_rate),
                        _("🔍 Cache size: {} entries").format(cache_size),
                        _("📞 Total requests: {}").format(total_requests),
                        _("🛰️ Rytec channels: {}").format(rytec_channels),
                        _("📺 DVB channels: {}").format(dvb_channels)
                    ])

            elif conversion_type in ["tv_to_m3u", "json_to_m3u", "xspf_to_m3u"]:
                total_channels = stats_data.get('total_channels', 0)
                output_file = stats_data.get('output_file', '')
                file_size = stats_data.get('file_size', 0)

                stats_message.extend([
                    _("📊 Channels converted: {}").format(total_channels),
                    _("💾 Output file: {}").format(output_file),
                    _("📁 File size: {}").format(self._format_file_size(file_size))
                ])

            elif conversion_type == "m3u_to_json":
                total_channels = stats_data.get('total_channels', 0)
                output_file = stats_data.get('output_file', '')
                file_size = stats_data.get('file_size', 0)

                stats_message.extend([
                    _("📊 Channels converted: {}").format(total_channels),
                    _("💾 Output file: {}").format(output_file),
                    _("📁 File size: {}").format(self._format_file_size(file_size)),
                    _("🎯 JSON structure: {}").format(stats_data.get('json_structure', 'playlist'))
                ])

            stats_message.extend([
                "",
                _("⏱️ Conversion type: {}").format(conversion_type),
                _("🕒 Completed at: {}").format(stats_data.get('timestamp', '')),
                _("✅ Status: Completed successfully")
            ])

        except Exception as e:
            logger.error(f"Error generating stats message: {str(e)}")
            stats_message.append(_("❌ Error generating statistics"))

        self.session.open(
            MessageBox,
            "\n".join(stats_message),
            MessageBox.TYPE_INFO,
            timeout=15
        )

    def print_performance_stats(self):
        """Stampa statistiche performance"""
        if hasattr(self, 'epg_mapper') and hasattr(self.epg_mapper, 'channel_cache'):
            cache = self.epg_mapper.channel_cache
            print("=== PERFORMANCE STATS ===")
            print(f"Channel cache hits: {cache._hits}")
            print(f"Channel cache misses: {cache._misses}")
            hit_rate = (cache._hits / (cache._hits + cache._misses)) * 100 if (cache._hits + cache._misses) > 0 else 0
            print(f"Channel cache hit rate: {hit_rate:.1f}%")

    def _show_plugin_information(self):
        """Show plugin information and credits."""
        info = [
            f"Archimede Universal Converter v.{CURRENT_VERSION}",
            _("Author: Lululla"),
            _("License: CC BY-NC-SA 4.0"),
            _("Developed for Enigma2"),
            _(f"Last modified: {LAST_MODIFIED_DATE}"),
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
        """Show info message and log it.

        Args:
            message (str): Message to show
        """
        logger.info(message)
        self.session.open(
            MessageBox,
            message,
            MessageBox.TYPE_INFO,
            timeout=5
        )
        self["status"].setText(message)

    def _show_error_message(self, message):
        """Show error message and log it.

        Args:
            message (str): Error message
        """
        logger.error(message)
        self.session.open(
            MessageBox,
            message,
            MessageBox.TYPE_ERROR,
            timeout=5
        )
        self["status"].setText(message)


class M3UConverterSettings(Setup):
    """Settings screen for M3U Converter plugin."""

    def __init__(self, session, parent=None):
        """Initialize settings screen.

        Args:
            session: Enigma2 session
            parent: Parent screen
        """
        Setup.__init__(self, session, setup="M3UConverterSettings", plugin="Extensions/M3UConverter")
        self.parent = parent

    def keySave(self):
        """Handle save action for settings."""
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


# Global converter instance
core_converter = CoreConverter()
