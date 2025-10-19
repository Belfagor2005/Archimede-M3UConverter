```markdown
# 📺 Archimede Universal Converter

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
- **M3U ➔ Enigma2 Bouquets** with EPG mapping
- **Enigma2 Bouquets ➔ M3U** format
- **JSON ➔ Enigma2 Bouquets** with metadata preservation
- **JSON ➔ M3U** playlist conversion
- **XSPF ➔ M3U** playlist conversion
- **M3U ➔ JSON** structured metadata export

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

### Troubleshooting
- **Check Logs**: Enable debug mode for detailed logging
- **Verify EPG**: Use analysis tools to check EPG coverage
- **Manual Correction**: Use editor for problematic channels and save to database
- **Service Reload**: Force reload if bouquets don't appear

## 🗂️ Supported Attributes

### M3U EXTINF Attributes
- `tvg-id`: Channel identifier for EPG matching
- `tvg-name`: Channel name for display
- `tvg-logo`: Channel logo URL
- `group-title`: Channel group/category
- `tvg-language`: Channel language
- `user-agent`: Custom user agent for streams

### JSON Structure
```json
{
  "name": "Channel Name",
  "url": "stream_url",
  "group": "Group Name", 
  "tvg_id": "channel.id",
  "logo": "logo_url",
  "tvg_name": "Display Name"
}
```

## 📊 Conversion Statistics

The plugin provides detailed analytics:
- **Total Channels Processed**
- **EPG Match Success Rate**
- **Database-specific Match Counts**
- **Cache Performance Metrics**
- **Effective EPG Coverage** (based on selected mode)

## 🔒 Backup & Safety

### Automatic Protection
- **Pre-conversion Backup**: Automatic bouquet backup
- **Rollback Capability**: Restore from backup on failure
- **Multiple Backups**: Configurable backup retention
- **Safe File Operations**: Transactional file writing

### Error Handling
- **Graceful Failure**: Continue processing on individual errors
- **Detailed Error Reporting**: Specific error messages and solutions
- **Recovery Options**: Multiple fallback strategies
- **Validation Checks**: Pre-conversion validation

## 📄 Credits & License

**Archimede Universal Converter v2.4**  
Created by **Lululla** (@Belfagor2005)

### 📜 License
- **CC BY-NC-SA 4.0**: Creative Commons Attribution-NonCommercial-ShareAlike
- **Redistribution**: Only with proper attribution
- **Modifications**: Must maintain credit header
- **Commercial Use**: Not permitted without authorization

### 🙏 Acknowledgments
- Enigma2 Community for testing and feedback
- EPGShare for EPG data sources
- Rytec for channel mapping database

---

**Made with ❤️ for the Enigma2 Community**  
*Keep your playlists organized and your EPG accurate!*
```
