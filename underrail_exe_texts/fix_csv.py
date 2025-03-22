import csv
from pathlib import Path

import typer
from rich import print as rprint


def main(input_csv: Path, output_csv: Path):
    """
    Reads a CSV file from the input path and writes its contents to the output path.
    This can be useful when reading the CSV file that was generated with the script underrail_exe_texts.csx.
    """
    if not input_csv.is_file():
        rprint(f"[red]Error:[/red] The input file does not exist: {input_csv}")
        raise typer.Exit(code=1)

    try:
        with input_csv.open("r", newline="", encoding="utf-8") as f_in:
            csv_reader = csv.reader(f_in)
            rows = list(csv_reader)
            rprint(f"[blue]Read {len(rows)} rows from {input_csv}[/blue]")
    except Exception as e:
        rprint(f"[red]Error reading file {input_csv}: {e}[/red]")
        raise typer.Exit(code=1)

    try:
        with output_csv.open("w", newline="", encoding="utf-8") as f_out:
            csv_writer = csv.writer(f_out)
            csv_writer.writerows(rows)
            rprint(
                f"[green]Successfully wrote {len(rows)} rows to {output_csv}[/green]"
            )
    except Exception as e:
        rprint(f"[red]Error writing file {output_csv}: {e}[/red]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
