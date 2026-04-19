<h1 align="center">📺 Archimede Universal Converter</h1>

![Visitors](https://komarev.com/ghpvc/?username=Belfagor2005&label=Repository%20Views&color=blueviolet)
![Version](https://img.shields.io/badge/Version-3.3-blue.svg)
[![Enigma2](https://img.shields.io/badge/Enigma2-Plugin-ff6600.svg)](https://www.enigma2.net)
![Python](https://img.shields.io/badge/Python3-only-orange.svg)
![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-green.svg)
[![Donate](https://img.shields.io/badge/_-Donate-red.svg?logo=githubsponsors&labelColor=555555&style=for-the-badge)](Maintainers.md#maintainers "Donate")

**Universal playlist converter for Enigma2 (Python 3 only)**  
Convert between M3U, JSON, XSPF formats and Enigma2 bouquets with advanced EPG mapping and intelligent channel matching.


## 🚀 Key Features

### 🎯 Enhanced Matching System
- **Configurable Similarity Thresholds**: Global, Rytec-specific, and DVB-specific matching (20-100%)
- **Advanced Manual Database**: Persistent storage for manual corrections with usage tracking
- **Smart Database Management**: Automatic cleanup and optimization of manual mappings
- **Real-time Analytics**: Enhanced statistics with detailed coverage breakdowns

### 🔧 Advanced Tools & Interface
- **Manual Database Editor**: Visual management interface for all manual corrections
- **Enhanced Tools Menu**: Comprehensive utilities including export/import functionality
- **Batch Processing Optimization**: Improved 50-channel batch processing for better performance
- **Memory Management**: Automatic cleanup when storage space is low

### 🛰️ EPG System
- **Intelligent Channel Matching**: Automatic mapping between IPTV channels and DVB/Satellite services
- **Multi-Database Support**: Rytec, DVB-S/C/T, and local bouquet databases
- **EPGShare Integration**: Download and parse EPG data from online sources
- **Manual Match Editor**: Fine-tune channel mappings with visual interface

### ⚙️ Core Features
- **Unified Channel Mapping**: Single structure for all service references
- **Cache Optimization**: Improved performance with smart caching
- **Database Mode Selection**: Choose between Full, DVB-only, Rytec-only, or DTT-only modes
- **Real-time Statistics**: Detailed conversion analytics and coverage reports

## 📋 Core Capabilities

### 🔄 Conversion Capabilities
- **M3U → Enigma2 Bouquets** with EPG mapping
- **Enigma2 Bouquets → M3U** format
- **Enigma2 Bouquets → Enigma2 Bouquets** with EPG mapping
- **JSON → Enigma2 Bouquets** with metadata preservation
- **JSON → M3U** playlist conversion
- **M3U → JSON** structured metadata export
- **XSPF → M3U** playlist conversion
- **Remove M3U Bouquets** - Complete cleanup

### 🛰️ EPG & Service Mapping
- **Automatic Service Matching**: Intelligent mapping using multiple algorithms
- **Rytec Database Integration**: Leverage existing Rytec channel mappings
- **DVB Service Detection**: Match with local DVB-S/C/T services
- **Multi-Language EPG**: Support for international EPG sources
- **EPGShare Compatibility**: Download and use EPGShare data sources

### ⚙️ Smart Processing
- **Group Management**: Automatic channel grouping with clean names
- **Quality Filtering**: Remove quality indicators for better matching
- **Binary Data Protection**: Filter out corrupted or binary data
- **Large File Handling**: Efficient processing of massive playlists
- **Backup System**: Automatic backup and restore functionality

## 🔍 Duplicates Management

### 🎯 Smart Duplicates Detection
- **Automatic Detection**: Find duplicate channels based on clean names
- **Grouped Display**: Organize duplicates by channel groups
- **Selection Mode**: Safe multi-select with checkbox interface
- **Bulk Operations**: Delete multiple duplicates at once

### 🎮 Duplicates Interface Controls

| Button | Action in Duplicates View |
|--------|---------------------------|
| **🔵 BLUE** | Toggle between All Mappings and Duplicates view |
| **🟢 GREEN** | Enter/Exit Selection Mode (shows/hides checkboxes) |
| **🟡 YELLOW** | Delete selected duplicates |
| **⏺️ OK** | Select/Deselect individual items |
| **🔴 RED** | Return to normal view/Close |

### 📋 Duplicates Workflow

#### Step 1: Access Duplicates View
- Press **BLUE** → "Duplicates" to enter duplicates management
- View all duplicate channel groups with counts

#### Step 2: Activate Selection Mode  
- Press **GREEN** → "Select" to enter selection mode
- Checkboxes `[ ]` appear next to each channel
- Status: "Selection mode active. Select items with OK"

#### Step 3: Select Duplicates to Remove
- Press **OK** on each duplicate you want to remove
- Checkboxes change to `[X]` for selected items
- Keep only the correct version of each channel

#### Step 4: Delete Selected Items
- Press **YELLOW** → "Delete" to remove selected duplicates
- Confirm deletion in the popup dialog
- **Remain in selection mode** for continued operations

#### Step 5: Continue or Exit
- **Stay in selection mode** - select more items if needed
- Press **GREEN** again → "Done" to exit selection mode
- Press **BLUE** → "All Mappings" to return to normal view

### 💡 Duplicates Management Tips
- **Keep**: Exact matches and manually corrected versions
- **Remove**: Fuzzy matches and incorrectly capitalized versions  
- **Verify**: Check service references before deletion
- **Batch Process**: Handle multiple groups without leaving selection mode

## 🎮 How to Use

### Main Interface Controls

| Button | Action |
|--------|--------|
| 🔴 **RED** | Close current screen |
| 🟢 **GREEN** | Select/Start conversion |
| 🟡 **YELLOW** | Remove M3U Bouquets |
| 🔵 **BLUE** | EPGImporter Settings |
| 📋 **MENU** | Plugin configuration |

### Conversion Screen Controls

| Button | Action |
|--------|--------|
| 🔴 **RED** | Open file browser |
| 🟢 **GREEN** | Start conversion process |
| 🟡 **YELLOW** | Open Manual Match Editor |
| 🔵 **BLUE** | Tools menu (context-aware) |
| ▶️ **OK** | Play selected channel stream |
| ⏹️ **STOP** | Stop media playback |
| ❌ **CANCEL** | Close plugin |

### Manual Match Editor Controls

| Button | Action |
|--------|--------|
| 🔴 **RED** | Close editor |
| 🟢 **GREEN** | Assign selected match |
| 🟡 **YELLOW** | Reset channel match |
| 🔵 **BLUE** | Save all changes |
| ▶️ **OK** | Select channel/match |
| 🔄 **ARROWS** | Navigate between lists |

### Manual Database Editor Controls

| Button | Action |
|--------|--------|
| 🔴 **RED** | Close editor |
| 🟢 **GREEN** | Toggle Selection Mode |
| 🟡 **YELLOW** | Delete selected items |
| 🔵 **BLUE** | Toggle Duplicates View |
| ▶️ **OK** | Select/Deselect items |
| ❌ **CANCEL** | Navigate back/Close |

## ⚙️ Configuration Options

### EPG Settings
- **EPG Generation**: Enable/disable EPG data attachment
- **Database Mode**: 
  - `Full`: DVB + Rytec + DTT (Complete)
  - `Both`: DVB + Rytec 
  - `DVB`: Only DVB services
  - `Rytec`: Only Rytec database
  - `DTT`: Only DVB-T services
- **EPG Source**: EPGShare or Standard mode
- **Language**: Country-specific EPG data

### Matching Settings
- **Similarity Threshold**: Global matching sensitivity (20-100%)
- **Rytec Similarity**: Specific threshold for Rytec database matching
- **DVB Similarity**: Specific threshold for DVB service matching
- **Manual Database**: Enable/disable use of manual corrections

### Bouquet Settings
- **Bouquet Mode**: Single bouquet or Multiple bouquets by group
- **Position**: Top or Bottom of bouquet list
- **Auto-reload**: Automatic service reload after conversion
- **Backup**: Enable automatic backups

### Processing Options
- **HLS Conversion**: Auto-convert to HLS format
- **Auto-open Editor**: Open manual editor after conversion
- **Debug Mode**: Enhanced logging and analysis
- **Max Backups**: Number of backups to retain

## 📊 EPG Matching System

### Matching Priority
1. **Manual Database**: Previously saved manual corrections
2. **Exact TVG-ID Match**: Perfect match with Rytec database
3. **Clean Name Match**: Normalized channel name matching
4. **Similarity Match**: Fuzzy matching for similar names
5. **DVB Service Match**: Local DVB service matching
6. **Fallback**: IPTV service reference generation

### Database Integration
- **Rytec Channels**: `/etc/epgimport/rytec.channels.xml`
- **DVB Services**: `/etc/enigma2/lamedb` and `lamedb5`
- **Existing Bouquets**: Current Enigma2 bouquet analysis
- **EPGShare**: Online EPG data download and parsing
- **Manual Database**: Persistent storage of user corrections

## 📁 File Structure

### Input Formats
- **M3U/M3U8**: Standard M3U playlists with EXTINF attributes
- **JSON**: Structured channel data with metadata
- **XSPF**: XML Shareable Playlist Format
- **TV Bouquets**: Enigma2 bouquet files

### Output Locations
- **Bouquets**: `/etc/enigma2/userbouquet.*.tv`
- **EPG Files**: `/etc/epgimport/*.channels.xml`
- **EPG Sources**: `/etc/epgimport/ArchimedeConverter.sources.xml`
- **Exports**: `/tmp/archimede_converter/exported_*`
- **Backups**: `/tmp/archimede_converter/archimede_backup/`
- **Logs**: `/tmp/archimede_converter/archimede_debug/`
- **Manual Database**: `/usr/lib/enigma2/python/Plugins/Extensions/M3UConverter/archimede_manual_mappings.json`

## 🔧 Advanced Tools

### Manual Match Editor
- **Visual Interface**: Side-by-side channel and match lists
- **Smart Suggestions**: AI-powered match recommendations
- **Similarity Scoring**: Percentage-based match quality
- **Priority Sorting**: Best matches shown first
- **Custom Mapping**: Save manual corrections for future use

### Database Management
- **Manual Database Editor**: Visual interface for managing all corrections
- **Export/Import**: Backup and restore manual mappings
- **Usage Statistics**: Track how often corrections are used
- **Automatic Cleanup**: Remove old or unused entries
- **Duplicates Management**: Find and remove duplicate mappings

### Analysis Tools
- **Cache Statistics**: Hit rates, performance metrics
- **Coverage Reports**: EPG matching success rates
- **Database Status**: Loaded channels and services count
- **Conversion Analytics**: Detailed conversion statistics

### Maintenance Tools
- **Bouquet Cleanup**: Remove all M3U-generated bouquets
- **EPG Cache Clear**: Reset EPG matching cache
- **Service Reload**: Force Enigma2 service reload
- **Backup Management**: Manual backup creation
- **Duplicates Cleanup**: Remove duplicate channel mappings

## 💡 Pro Tips

### Optimal Configuration
1. **For Satellite Users**: Use `Full` or `Both` database mode with 80% similarity
2. **For IPTV-Only**: Use `Rytec` mode with EPGShare and 70% similarity
3. **For Large Lists**: Enable debug mode for performance analysis
4. **For Accuracy**: Use Manual Match Editor and save corrections to database

### Performance Optimization
- **Cache Size**: Automatic management for optimal performance
- **Batch Processing**: Efficient 50-channel batch processing
- **Memory Management**: Smart cleanup when storage is low
- **Incremental Parsing**: Handle very large files efficiently
- **Regular Duplicates Cleanup**: Maintain database efficiency

### Troubleshooting
- **Check Logs**: Enable debug mode for detailed logging
- **Verify EPG**: Use analysis tools to check EPG coverage
- **Manual Correction**: Use editor for problematic channels and save to database
- **Service Reload**: Force reload if bouquets don't appear
- **Duplicates Check**: Regularly clean duplicates for better performance

## 🗂️ Supported Attributes

### M3U EXTINF Attributes
- `tvg-id`: Channel identifier for EPG matching
- `tvg-name`: Channel name for display
- `tvg-logo`: Channel logo URL
- `group-title`: Channel group/category
- `tvg-language`: Channel language
- `user-agent`: Custom user agent for streams

## 📊 Output & Export Features

### 🔄 Export Capabilities
- **Output Location**: `/tmp/exported_*.m3u` (automatically timestamped)
- **Content Preservation**: Each channel retains original name and streaming URL
- **Smart Filtering**: Only valid IPTV services included (`#SERVICE 4097`, `5001`, etc.)
- **Service Exclusion**: Non-stream services (DVB, radio, PVR) automatically filtered
- **Backup Utility**: Perfect for creating backup playlists or external editing

## 🎯 System Requirements & Architecture
- **Python 3 Exclusive**: Designed for modern Enigma2 images only
- **Modern Hardware**: Optimized for current-generation Enigma2 receivers
- **Offline Operation**: No internet connection required for conversion
- **Privacy Focused**: Clean local processing — no logging or tracking

## 🏗️ Technical Excellence
- **Solid Architecture**: Well-structured code with clear separation of responsibilities
- **Comprehensive Format Support**: M3U↔TV, JSON↔TV, XSPF→M3U, and more
- **Hybrid Service Reference System**: Intelligent handling of mixed service types
- **Multi-Storage Detection**: Auto-detection of mounted storage devices

## 📺 Playback & Video Management
- **Aspect Ratio Intelligence**: Automatic management of video aspect ratios during playback
- **HLS Native Support**: Built-in conversion and support for HLS streams
- **Binary Data Protection**: Advanced filtering against data corruption
- **Seamless Integration**: Direct channel playback from editor interface

## 🛰️ Advanced EPG System
- **Multi-Source EPG**: Support for multiple EPG sources with mirroring capabilities
- **Rytec Integration**: Automatic channel mapping with Rytec database support
- **Intelligent Matching**: Similarity-based matching with configurable thresholds (20-100%)
- **Multi-Database Modes**: Full (DVB+Rytec+DVB-T), Both, DVB Only, Rytec Only, DTT Only

## 🔧 Professional Tools Suite
- **Manual EPG Match Editor**: Visual interface for precise channel matching
- **Real-time Analytics**: Detailed EPG coverage statistics and cache performance metrics
- **Database Management**: Persistent storage and management of manual corrections
- **Export/Import System**: Comprehensive backup and restore functionality for mappings

## 🎮 Enhanced User Experience
- **Adaptive Interface**: Professional UI that adapts to different screen resolutions
- **Multi-language Support**: Comprehensive European language coverage
- **Batch Processing**: Optimized 50-channel batches for superior performance
- **Context-Aware Tools**: Intelligent tools menu with relevant options

## 🛡️ Reliability & Maintenance
- **Robust Error Handling**: Automatic backup and recovery mechanisms
- **Memory Optimization**: Smart cleanup when storage space is low
- **Automatic Maintenance**: Database cleanup and optimization routines
- **Enhanced Debugging**: Detailed logging and analysis tools for troubleshooting

## 📈 Performance Features
- **Efficient Processing**: Large file handling with incremental parsing
- **Cache Optimization**: Smart caching system for improved performance
- **Resource Management**: Automatic memory and storage management
- **Quality Filtering**: Intelligent removal of quality indicators for better matching

## 🔄 Workflow Excellence
- **Backup Integration**: Built-in backup system with rollback capability
- **Service Reload**: Automatic Enigma2 service reload after operations
- **Duplicate Management**: Advanced detection and removal of duplicate channels
- **Real-time Feedback**: Immediate status updates and progress indicators

## 📊 Conversion Statistics
The plugin provides detailed analytics:
- **Total Channels Processed**
- **EPG Match Success Rate**
- **Database-specific Match Counts**
- **Cache Performance Metrics**
- **Effective EPG Coverage** (based on selected mode)
- **Duplicates Found and Removed**

## 🔒 Backup & Safety

### Automatic Protection
- **Pre-conversion Backup**: Automatic bouquet backup
- **Rollback Capability**: Restore from backup on failure
- **Multiple Backups**: Configurable backup retention
- **Safe File Operations**: Transactional file writing
- **Selection Mode Safety**: Prevents accidental deletions

### Error Handling
- **Graceful Failure**: Continue processing on individual errors
- **Detailed Error Reporting**: Specific error messages and solutions
- **Recovery Options**: Multiple fallback strategies
- **Validation Checks**: Pre-conversion validation
- **Duplicates Protection**: Confirmation before bulk deletions

## 📄 Credits & License

### Developer
- **Archimede Universal Converter v3.0**
- **Created by Lululla** ([@Belfagor2005](https://github.com/Belfagor2005))
- **Last Modified**: 2025-11-02

### 📜 License
- **CC BY-NC-SA 4.0**: Creative Commons Attribution-NonCommercial-ShareAlike
- **Redistribution**: Only with proper attribution
- **Modifications**: Must maintain credit header
- **Commercial Use**: Not permitted without authorization

### 🙏 Acknowledgments
- **Enigma2 Community** for testing and feedback
- **EPGShare** for EPG data sources
- **Rytec** for channel mapping database

### 💝 Supporting
If you like this plugin, consider supporting the development!

**☕ Offer Coffee** → [paypal.com/paypalme/belfagor2005](https://paypal.com/paypalme/belfagor2005)  
**🍺 Offer Beer** → [ko-fi.com/lululla](https://ko-fi.com/lululla)

**Support on**: [www.corvoboys.org](http://www.corvoboys.org) • [www.linuxsat-support.com](http://www.linuxsat-support.com)

---

## Made with ❤️ for the Enigma2 Community


## Made with ❤️ for the Enigma2 Community
