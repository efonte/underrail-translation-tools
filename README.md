# UnderRail Translation Tools

UnderRail Translation Tools is a Python-based suite for extracting and injecting translations for the game UnderRail. It decodes the game's UDLG files into JSON, extracts translatable texts into CSV files for translation, and then rebuilds the game files with your updated translations. The tool supports batch processing, preserves the original file structure, and automatically handles compressed payloads.

## Features

It decodes UDLG files by first validating the UDLG signature and then processing records that include support for gzip-compressed payloads. It can extract translatable texts in two modes:

- `english` mode where strings following an English marker are processed.
- `variables` mode where variable-text pairs are handled.

The encoding functionality injects translations from a CSV into the decoded JSON and rebuilds the UDLG files. In addition, the tool includes a merge command that unifies existing translations from a base CSV with new extractions from updated game files. Batch processing is available for both decoding and encoding, ensuring that the game’s folder structure is maintained.

## Requirements

- Python 3.8+
- Required Python packages:

  ```bash
  pip install typer rich
  ```

## Usage

**Decode UDLG Files to JSON**

This command decodes UDLG files into JSON and optionally extracts texts to CSV. It checks for a valid UDLG signature, handles decompression if needed, and supports both single file and directory processing.

Basic decoding:

```bash
python udlg_tools.py decode "UnderRail/data/dialogs" -o "output/dialogs_json"
```

To extract texts into a CSV in default English mode:

```bash
python udlg_tools.py decode "UnderRail/data/dialogs" -o "output/dialogs_json" --csv
```

To include the file path in the CSV:

```bash
python udlg_tools.py decode "UnderRail/data/dialogs" -o "output/dialogs_json" --csv --include-file
```

To use variable mode instead of English mode:

```bash
python udlg_tools.py decode "UnderRail/data/dialogs" -o "output/dialogs_json" --csv -m "variables"
```

**Encode JSON Back to UDLG Files**

This command encodes a JSON file (or a directory of JSON files) back into the UDLG format. It can also inject translations from a CSV file.

Basic encoding:

```bash
python udlg_tools.py encode "output/dialogs_json" -o "output/dialogs_generated"
```

To apply updated translations from a CSV:

```bash
python udlg_tools.py encode "output/dialogs_json" -o "output/dialogs_generated" --csv "translations.csv"
```

Additional options such as including the file path and mode selection (english/variables) can be specified:

```bash
python udlg_tools.py encode "output/dialogs_json" -o "output/dialogs_generated" --csv "translations.csv" --include-file -m "variables"
```

**Merge Existing Translations**

Use the merge command to combine translations from an existing base CSV with new extractions from updated game files. This process unifies the CSV columns into a standard format, ensuring that updated translations are applied where available.

```bash
python udlg_tools.py merge-csv "old_translations.csv" "new_extracted.csv" "merged_output.csv"
```

## Translation Workflow

1. **Extract the original texts:**
   Run the decode command to generate JSON files and a CSV file containing all translatable texts. For example:

   ```bash
   python udlg_tools.py decode "UnderRail/data/dialogs" -o "extracted" --csv
   ```

2. **Translate the texts:**
   Open the generated CSV file. In English mode, the CSV includes columns for "Original" and "Translation" (or "File", "Original", and "Translation" if file paths are included). In variables mode, the CSV contains variable names, original texts, and translations. Edit the "Translation" column with your translated text and save your changes.

3. **Inject the translations:**
   Use the encode command to rebuild the UDLG files with your translations:

   ```bash
   python udlg_tools.py encode "extracted" -o "translated" --csv "translations.csv"
   ```

4. **Merge translations (if needed):**
   If you maintain an existing translation base, merge it with the new CSV:

   ```bash
   python udlg_tools.py merge-csv "old_translations.csv" "new_extracted.csv" "merged_output.csv"
   ```

5. **Deploy the updated game files:**
   Copy the generated UDLG files back into the game directory, making sure to back up the originals first.

## File Structure

The tool works with three main file types:

- **.udlg:** Original game files in UDLG format (binary files with a specific signature)
- **.json:** Decoded representations of the UDLG files, allowing easy text manipulation
- **.csv:** CSV files either extracted for translation or used to inject translations back into the JSON data

The original file hierarchy is preserved during both the decode and encode processes.

## Notes

- Always back up your game files before replacing them with translated versions.
- Untranslated texts will remain in their original form.
- The tool automatically handles compressed payloads by decompressing gzip zlib data on read and recompressing on write.
- The merge functionality is especially useful when game content is updated and you want to keep older translations.
