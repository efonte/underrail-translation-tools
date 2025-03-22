#r "nuget: dnlib, 4.4.0"
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Security.Cryptography;
using dnlib.DotNet;
using dnlib.DotNet.Emit;

// Top-level statements
var args = Args.ToArray();
if (args.Length == 0)
{
  PrintUsage();
}
else
{
  string command = args[0].ToLower();
  try
  {
    if (command == "extract")
      RunExtraction(args);
    else if (command == "repack")
      RunRepacking(args);
    else
    {
      Console.WriteLine("Unknown command: " + command);
      PrintUsage();
    }
  }
  catch (Exception ex)
  {
    Console.WriteLine("An error occurred: " + ex.Message);
  }
}

static void PrintUsage()
{
  Console.WriteLine("Usage:");
  Console.WriteLine("  dotnet run extract [exePath] [dialogueCSV] [--include-single]");
  Console.WriteLine("  dotnet run repack [exePath] [dialogueCSV] [outputFolder] [--include-single]");
  Console.WriteLine();
  Console.WriteLine("Defaults:");
  Console.WriteLine("  exePath: underrail.exe (in current directory)");
  Console.WriteLine("  dialogueCSV: exePath + \"_texts.csv\"");
  Console.WriteLine("  outputFolder: repacked");
}

static void RunExtraction(string[] args)
{
  // Parse positional parameters and optional flags:
  string exePath = "underrail.exe";
  string csvPath = null;
  bool includeSingle = false;

  // Obtain arguments that are not flags (those that do not start with "--")
  var nonFlagArgs = args.Where(a => !a.StartsWith("--")).ToArray();
  if (nonFlagArgs.Length >= 2)
    exePath = nonFlagArgs[1];
  if (nonFlagArgs.Length >= 3)
    csvPath = nonFlagArgs[2];
  else
    csvPath = exePath + "_texts.csv";

  // Check optional flags.
  foreach (var arg in args)
  {
    if (arg.ToLower() == "--include-single")
    {
      includeSingle = true;
    }
  }

  if (!File.Exists(exePath))
  {
    Console.WriteLine("Executable file not found: " + exePath);
    return;
  }

  ExePatcher patcher = new ExePatcher(exePath)
  {
    IncludeSingleWord = includeSingle
  };
  patcher.ExportCSV(csvPath);
  Console.WriteLine("Extraction completed. Dialogues saved to: " + csvPath);
}

static void RunRepacking(string[] args)
{
  string exePath = "underrail.exe";
  string csvPath = null;
  string outputFolder = "repacked";
  bool includeSingle = false;

  var nonFlagArgs = args.Where(a => !a.StartsWith("--")).ToArray();
  if (nonFlagArgs.Length >= 2)
    exePath = nonFlagArgs[1];
  if (nonFlagArgs.Length >= 3)
    csvPath = nonFlagArgs[2];
  else
    csvPath = exePath + "_texts.csv";
  if (nonFlagArgs.Length >= 4)
    outputFolder = nonFlagArgs[3];

  foreach (var arg in args)
  {
    if (arg.ToLower() == "--include-single")
    {
      includeSingle = true;
    }
  }

  if (!File.Exists(exePath))
  {
    Console.WriteLine("Executable file not found: " + exePath);
    return;
  }
  if (!File.Exists(csvPath))
  {
    Console.WriteLine("Dialogue CSV file not found: " + csvPath);
    return;
  }

  ExePatcher patcher = new ExePatcher(exePath)
  {
    IncludeSingleWord = includeSingle
  };
  patcher.ImportAndRebuild(csvPath, outputFolder);
  string outputPath = Path.Combine(outputFolder, patcher.RelativePath);
  Console.WriteLine("Repack completed. New executable saved to: " + outputPath);
}

//
// Represents an entry for a dialogue text.
//
class DialogueEntry
{
  public string Key { get; set; }
  public string Original { get; set; }
  public string Translation { get; set; }
}

//
// This class extracts texts from an executable (from custom attributes, constant string fields,
// and Ldstr instructions) and rebuilds the executable with new translations read from a CSV file.
// Each extracted text is given a stable key generated solely from its original text using MD5 hashing.
// This approach ensures that if the executable is updated or obfuscated, as long as the dialogue text remains
// unchanged, the translation will still be applied.
// If the dialogue text changes, then the translation is not applied and retranslation is required.
//
class ExePatcher
{
  public string ExePath { get; }
  public string RelativePath => Path.GetFileName(ExePath);
  public bool IncludeSingleWord { get; set; } = false;

  public ExePatcher(string exePath)
  {
    ExePath = exePath;
  }

  // Extract dialogue texts from custom attributes, fields, and Ldstr instructions.
  public List<DialogueEntry> ExtractDialogues()
  {
    var dialogues = new Dictionary<string, DialogueEntry>();
    var module = ModuleDefMD.Load(ExePath);

    foreach (var type in module.Types)
    {
      // Extract texts from custom attributes.
      foreach (var attr in type.CustomAttributes)
      {
        for (int i = 0; i < attr.ConstructorArguments.Count; i++)
        {
          var arg = attr.ConstructorArguments[i];
          if (arg.Type.FullName == "System.String")
          {
            string text = "";
            if (arg.Value is UTF8String utf8)
              text = utf8.String;
            else if (arg.Value is string s)
              text = s;
            text = text.Replace("\r\n", "\\r\\n");

            if (!ShouldExtract(text))
              continue;

            string key = ComputeStableKey(text, "attr");
            if (!dialogues.ContainsKey(key))
            {
              dialogues[key] = new DialogueEntry { Key = key, Original = text, Translation = "" };
            }
          }
        }
      }

      // Extract texts from constant string fields.
      foreach (var field in type.Fields)
      {
        if (field.HasConstant && field.Constant.Value is string text)
        {
          text = text.Replace("\r\n", "\\r\\n");
          if (!ShouldExtract(text))
            continue;

          string key = ComputeStableKey(text, "const");
          if (!dialogues.ContainsKey(key))
          {
            dialogues[key] = new DialogueEntry { Key = key, Original = text, Translation = "" };
          }
        }
      }

      // Extract texts from Ldstr instructions in method bodies.
      foreach (var method in type.Methods)
      {
        if (!method.HasBody)
          continue;

        foreach (var instr in method.Body.Instructions)
        {
          if (instr.OpCode.Code != Code.Ldstr)
            continue;

          if (instr.Operand is string s)
          {
            string text = s.Replace("\r\n", "\\r\\n");
            if (!ShouldExtract(text))
              continue;

            string key = ComputeStableKey(text, "ldstr");
            if (!dialogues.ContainsKey(key))
            {
              dialogues[key] = new DialogueEntry { Key = key, Original = text, Translation = "" };
            }
          }
        }
      }
    }
    var list = dialogues.Values.ToList();
    list.Sort((a, b) => string.Compare(a.Original, b.Original, StringComparison.Ordinal));
    return list;
  }

  // Writes the extracted dialogues to a CSV file with header "Variable,Original,Translation".
  public void ExportCSV(string csvPath)
  {
    var dialogues = ExtractDialogues();
    using (var sw = new StreamWriter(csvPath, false, Encoding.UTF8))
    {
      sw.WriteLine("Variable,Original,Translation");
      foreach (var entry in dialogues)
      {
        string keyEscaped = EscapeCsv(entry.Key);
        string originalEscaped = EscapeCsv(entry.Original);
        sw.WriteLine($"{keyEscaped},{originalEscaped},");
      }
    }
  }

  // Reads translations from a CSV file and rebuilds the executable with updated texts.
  // For each dialogue, if the original text in the executable matches the one exported in CSV,
  // the translation is applied.
  public void ImportAndRebuild(string csvPath, string outputFolder)
  {
    if (!File.Exists(csvPath))
      throw new FileNotFoundException("CSV file not found.", csvPath);

    var translations = new Dictionary<string, (string Original, string Translation)>();
    using (var sr = new StreamReader(csvPath, Encoding.UTF8))
    {
      // Skip header line.
      string header = sr.ReadLine();
      while (!sr.EndOfStream)
      {
        string line = sr.ReadLine();
        if (string.IsNullOrWhiteSpace(line))
          continue;

        var fields = ParseCsvLine(line);
        if (fields.Length < 3)
          continue;

        string key = fields[0].Trim();
        string original = fields[1].Trim();
        string translation = fields[2].Trim();
        if (!string.IsNullOrWhiteSpace(translation))
          translations[key] = (original, translation);
      }
    }
    Rebuild(translations, outputFolder);
  }

  // Applies translations to custom attributes, constant fields, and Ldstr instructions,
  // then writes the new executable.
  private void Rebuild(Dictionary<string, (string Original, string Translation)> translations, string outputFolder)
  {
    string outputPath = Path.Combine(outputFolder, RelativePath);
    Directory.CreateDirectory(Path.GetDirectoryName(outputPath));

    var module = ModuleDefMD.Load(ExePath);

    foreach (var type in module.Types)
    {
      // Update texts in custom attributes.
      foreach (var attr in type.CustomAttributes)
      {
        for (int i = 0; i < attr.ConstructorArguments.Count; i++)
        {
          var arg = attr.ConstructorArguments[i];
          if (arg.Type.FullName != "System.String")
            continue;

          string original = "";
          if (arg.Value is UTF8String utf8)
            original = utf8.String;
          else if (arg.Value is string s)
            original = s;
          original = original.Replace("\r\n", "\\r\\n");

          if (!ShouldExtract(original))
            continue;

          string key = ComputeStableKey(original, "attr");
          if (translations.TryGetValue(key, out var tuple) && tuple.Original == original)
          {
            string newText = tuple.Translation.Replace("\\r\\n", "\r\n");
            arg.Value = new UTF8String(newText);
            attr.ConstructorArguments[i] = arg;
          }
        }
      }

      // Update texts in constant string fields.
      foreach (var field in type.Fields)
      {
        if (field.HasConstant && field.Constant.Value is string original)
        {
          original = original.Replace("\r\n", "\\r\\n");
          if (!ShouldExtract(original))
            continue;

          string key = ComputeStableKey(original, "const");
          if (translations.TryGetValue(key, out var tuple) && tuple.Original == original)
          {
            field.Constant.Value = tuple.Translation.Replace("\\r\\n", "\r\n");
          }
        }
      }

      // Update texts in Ldstr instructions.
      foreach (var method in type.Methods)
      {
        if (!method.HasBody)
          continue;

        foreach (var instr in method.Body.Instructions)
        {
          if (instr.OpCode.Code != Code.Ldstr)
            continue;

          if (instr.Operand is string s)
          {
            string original = s.Replace("\r\n", "\\r\\n");
            if (!ShouldExtract(original))
              continue;

            string key = ComputeStableKey(original, "ldstr");
            if (translations.TryGetValue(key, out var tuple) && tuple.Original == original)
            {
              instr.Operand = tuple.Translation.Replace("\\r\\n", "\r\n");
            }
          }
        }
      }
    }

    module.Write(outputPath);
  }

  // Determines if the text should be extracted.
  private bool ShouldExtract(string text)
  {
    if (string.IsNullOrWhiteSpace(text))
      return false;

    string trimmed = text.Trim();

    if (trimmed == "My Games\\Underrail")
      return false;

    if (trimmed.Contains(" "))
      return true; // Multi-word dialogues

    // For single words:
    if (!IncludeSingleWord)
      return false;

    if (trimmed.Length < 3)
      return false;

    if (trimmed.All(char.IsUpper))
      return false;

    if (trimmed.Skip(1).Any(char.IsUpper))
      return false;

    if (Regex.IsMatch(trimmed, @"[_\/\\;,:.-]"))
      return false;

    if (trimmed.Any(char.IsDigit))
      return false;

    if (Regex.IsMatch(trimmed, "^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$"))
      return false;

    if (trimmed.Length > 1 && char.IsLower(trimmed[0]) && trimmed.Skip(1).Any(c => char.IsUpper(c)))
      return false;

    if (!trimmed.Any(c => (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')))
      return false;

    return true;
  }

  // Escapes CSV fields that contain commas, quotes, or newlines.
  private static string EscapeCsv(string input)
  {
    if (input.Contains("\"") || input.Contains(",") || input.Contains("\n") || input.Contains("\r"))
    {
      input = input.Replace("\"", "\"\"");
      return $"\"{input}\"";
    }
    return input;
  }

  // A minimal CSV parser that handles quoted fields.
  private static string[] ParseCsvLine(string line)
  {
    var result = new List<string>();
    bool inQuotes = false;
    var field = new StringBuilder();

    for (int i = 0; i < line.Length; i++)
    {
      char c = line[i];
      if (c == '"')
      {
        if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
        {
          field.Append('"');
          i++;
        }
        else
        {
          inQuotes = !inQuotes;
        }
      }
      else if (c == ',' && !inQuotes)
      {
        result.Add(field.ToString());
        field.Clear();
      }
      else
      {
        field.Append(c);
      }
    }
    result.Add(field.ToString());
    return result.ToArray();
  }

  // Computes a stable key from the dialogue text using MD5 hash and a prefix.
  private static string ComputeStableKey(string text, string prefix)
  {
    using (MD5 md5 = MD5.Create())
    {
      byte[] hashBytes = md5.ComputeHash(Encoding.UTF8.GetBytes(text));
      string hash = BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
      return $"{prefix}.{hash}";
    }
  }
}
