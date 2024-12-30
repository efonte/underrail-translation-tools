# UnderRail Translation Tools

A Python-based tool for extracting and injecting translations for the game UnderRail. This tool allows you to decode the game's UDLG files into JSON format, extract texts for translation into CSV, and then rebuild the game files with your translations.

## Features

- Decode UDLG files to JSON format
- Extract translatable texts to CSV
- Inject translations back into the game files
- Merge existing translations with new game content
- Support for batch processing of multiple files
- Preserve game file structure and formatting

## Requirements

- Python 3.8+
- Required Python packages:

  ```bash
  pip install typer rich
  ```

## Usage

### Basic Commands

1. **Decode UDLG files to JSON:**

    ```bash
    python udlg_tools.py decode "UnderRail/data/dialogs" -o "output/dialogs_json"
    ```

2. **Extract texts to CSV:**

    ```bash
    python udlg_tools.py decode "UnderRail/data/dialogs" -o "output/dialogs_json" --csv
    ```

3. **Inject translations back:**

    ```bash
    python udlg_tools.py encode "output/dialogs_json" -o "output/dialogs_generated" --csv "translations.csv"
    ```

4. **Merge existing translations with new game content:**

    ```bash
    python udlg_tools.py merge-csv "old_translations.csv" "new_extracted.csv" "merged_output.csv"
    ```

### Translation Workflow

1. Extract original texts:

   ```bash
   python udlg_tools.py decode "UnderRail/data/dialogs" -o "Extracted" --csv
   ```

   This will create JSON files and a CSV file with all translatable texts.

2. Translate the texts:
   - Open the generated CSV file
   - The CSV contains two columns: "Original" and "Translation"
   - Translate the texts in the "Translation" column
   - Save the CSV file

3. Inject translations:

   ```bash
   python udlg_tools.py encode "Extracted" -o "Translated" --csv "translations.csv"
   ```

4. Copy the translated files back to the game directory

### Advanced Features

- Include file paths in CSV:

  ```bash
  python udlg_tools.py decode "input/path" --csv --include-file
  ```

- Process single file:

  ```bash
  python udlg_tools.py decode "path/to/single.udlg"
  ```

## File Structure

The tool works with three main file types:

- `.udlg`: Original game files
- `.json`: Decoded game files
- `.csv`: Extracted texts for translation

## Notes

- Always backup your game files before replacing them with translated versions
- The tool preserves the original file structure and formatting
- Untranslated texts will remain in their original form
- The merge feature helps maintain existing translations when game content is updated
