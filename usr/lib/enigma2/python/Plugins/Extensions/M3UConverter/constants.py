# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 3.0                                        #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "20:05 - 20251102"                    #
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

# ======================== IMPORTS ========================
# ?? STANDARD LIBRARIES (Python built-ins)
from os.path import dirname, join

# ?? ENIGMA2 CORE
from enigma import getDesktop

# ?? ENIGMA2 SCREENS
from Screens.Setup import Setup

# ?? LOCAL MODULES
from . import _
from .utils import get_best_storage_path
from .version import CURRENT_VERSION


# ==================== CONSTANTS & PATHS ====================
PLUGIN_TITLE = _(
    "Archimede Universal Converter v.%s by Lululla") % CURRENT_VERSION
PLUGIN_PATH = dirname(__file__)
BASE_STORAGE_PATH = get_best_storage_path()
ARCHIMEDE_M3U_PATH = join(BASE_STORAGE_PATH, "movie")
ARCHIMEDE_CONVERTER_PATH = join(BASE_STORAGE_PATH, "archimede_converter")
LOG_DIR = ARCHIMEDE_CONVERTER_PATH
DEBUG_DIR = join(ARCHIMEDE_CONVERTER_PATH, "debug")
MAIN_LOG = join(LOG_DIR, "converter.log")
DB_PATCH = join(PLUGIN_PATH, "database", "manual_mappings.json")
EXPORT_DIR = join(PLUGIN_PATH, "database")
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2


# Screen dimensions
screen_dimensions = getDesktop(0).size()
SCREEN_WIDTH = screen_dimensions.width()

if SCREEN_WIDTH > 1280:
    ITEM_HEIGHT = 120
    FONT_SIZE = 28
    ICON_SIZE = (30, 30)
else:
    ITEM_HEIGHT = 100
    FONT_SIZE = 24
    ICON_SIZE = (25, 25)


class M3UConverterSettings(Setup):
    """Settings screen for M3U Converter plugin."""

    def __init__(self, session, parent=None):
        """Initialize settings screen."""
        Setup.__init__(
            self,
            session,
            setup="M3UConverterSettings",
            plugin="Extensions/M3UConverter")
        self.parent = parent

    def keySave(self):
        """Handle save action for settings."""
        Setup.keySave(self)


# Language to country mapping for EPG sources
LANGUAGE_TO_COUNTRY = {
    # Europa
    'it': 'IT',    # Italian -> Italy
    'en': 'UK',    # English -> United Kingdom
    'de': 'DE',    # German -> Germany
    'fr': 'FR',    # French -> France
    'es': 'ES',    # Spanish -> Spain
    'pt': 'PT',    # Portuguese -> Portugal
    'nl': 'NL',    # Dutch -> Netherlands
    'gr': 'GR',    # Greek -> Greece
    'cz': 'CZ',    # Czech -> Czech Republic
    'hu': 'HU',    # Hungarian -> Hungary
    'ro': 'RO',    # Romanian -> Romania
    'se': 'SE',    # Swedish -> Sweden
    'no': 'NO',    # Norwegian -> Norway
    'dk': 'DK',    # Danish -> Denmark
    'fi': 'FI',    # Finnish -> Finland
    'at': 'AT',    # Austria
    'ba': 'BA',    # Bosnia and Herzegovina
    'al': 'AL',    # Albania
    'be': 'BE',    # Belgium
    'ch': 'CH',    # Switzerland
    'cy': 'CY',    # Cyprus - CORRETTO
    'hr': 'HR',    # Croatia
    'lt': 'LT',    # Lithuania
    'lv': 'LV',    # Latvia
    'mt': 'MT',    # Malta
    'pl': 'PL',    # Poland
    'rs': 'RS',    # Serbia
    'sk': 'SK',    # Slovakia
    'bg': 'BG',    # Bulgaria
    'tr': 'TR',    # Turkey

    # Americhe
    'us': 'US2',   # USA
    'usl': 'US_LOCALS1',  # USA Locals
    'uss': 'US_SPORTS1',  # USA Sports
    'ca': 'CA2',   # Canada
    'mx': 'MX1',   # Mexico
    'br': 'BR1',   # Brazil
    'br2': 'BR2',  # Brazil 2
    'cl': 'CL1',   # Chile
    'co': 'CO1',   # Colombia
    'cr': 'CR1',   # Costa Rica
    'do': 'DO1',   # Dominican Republic
    'ec': 'EC1',   # Ecuador
    'pe': 'PE1',   # Peru
    'uy': 'UY1',   # Uruguay
    'pa': 'PA1',   # Panama
    'ar': 'AR',    # Argentina
    'jm': 'JM1',   # Jamaica

    # Asia
    'as': 'AS',    # Asian Television - CORRETTO
    'in': 'IN1',   # India
    'in2': 'IN2',  # India 2
    'in4': 'IN4',  # India 4
    'jp': 'JP1',   # Japan
    'jp2': 'JP2',  # Japan 2
    'kr': 'KR1',   # Korea
    'hk': 'HK1',   # Hong Kong
    'id': 'ID1',   # Indonesia
    'my': 'MY1',   # Malaysia
    'ph': 'PH1',   # Philippines
    'ph2': 'PH2',  # Philippines 2
    'th': 'TH1',   # Thailand
    'vn': 'VN1',   # Vietnam
    'pk': 'PK1',   # Pakistan
    'il': 'IL1',   # Israel
    'sa': 'SA2',   # Saudi Arabia
    'sg': 'SG1',   # Singapore
    'mn': 'MN1',   # Mongolia
    'cn': 'AS',    # China -> Asian Television (CORRETTO)

    # Oceania
    'au': 'AU',    # Australia
    'nz': 'NZ1',   # New Zealand

    # Africa - Medio Oriente
    'eg': 'EG1',   # Egypt
    'za': 'ZA1',   # South Africa
    'ng': 'NG1',   # Nigeria
    'ke': 'KE1',   # Kenya
    'sa1': 'SA1',  # Saudi Arabia alt

    # Special Network
    'bein': 'BEIN',          # BEIN Sports
    'rakuten': 'RAKUTEN1',   # Rakuten TV
    'plex': 'PLEX1',         # Plex TV
    'distro': 'DISTROTV1',   # Distro TV
    'fanduel': 'FANDUEL1',   # FanDuel
    'draftkings': 'DRAFTKINGS1',  # DraftKings
    'powertv': 'POWERNATION1',    # PowerNation
    'peacock': 'PEACOCK1',        # Peacock
    'tbnplus': 'TBNPLUS1',        # TBN Plus
    'thesportplus': 'THESPORTPLUS1',  # The Sport Plus
    'rally': 'RALLY_TV1',         # Rally TV
    'sportklub': 'SPORTKLUB1',    # Sport Klub
    'voa': 'VOA1',                # Voice of America
    'aljazeera': 'ALJAZEERA1',    # Al Jazeera
    'viva': 'VIVA_RUSSIA',        # Viva Russia feed (CORRETTO)

    # Full
    'all': 'ALL'  # All sources
}
