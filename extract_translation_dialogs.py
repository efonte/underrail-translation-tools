#!/usr/bin/env python3
import csv
from pathlib import Path

import typer
from rich import print as rprint

app = typer.Typer(help="A CLI tool to extract and insert translations into a CSV file.")


@app.command()
def extract(
    csv_file: Path = typer.Option(
        Path("data.csv"), "--csv", "-c", help="Path to the CSV file."
    ),
    output: Path = typer.Option(
        Path("translations.txt"),
        "--output",
        "-o",
        help="Output file for extracted translations.",
    ),
):
    """
    Extract the 'Translation' column from the CSV file and save it to an output file.
    """
    if not csv_file.exists():
        rprint(f"[red]Error:[/red] CSV file not found: {csv_file}")
        raise typer.Exit(1)

    translations = []
    try:
        with csv_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None or "Translation" not in reader.fieldnames:
                rprint(
                    "[red]Error:[/red] CSV file does not contain 'Translation' header."
                )
                raise typer.Exit(1)
            for row in reader:
                translations.append(row["Translation"])
    except Exception as e:
        rprint(f"[red]Failed to read CSV:[/red] {e}")
        raise typer.Exit(1)

    try:
        output.write_text("\n".join(translations), encoding="utf-8")
    except Exception as e:
        rprint(f"[red]Failed to write translation file:[/red] {e}")
        raise typer.Exit(1)

    rprint(f"[green]Success:[/green] Extracted translations written to {output}")


@app.command()
def insert(
    csv_file: Path = typer.Option(
        Path("data.csv"), "--csv", "-c", help="Path to the CSV file."
    ),
    translation_file: Path = typer.Option(
        Path("translations.txt"),
        "--input",
        "-i",
        help="Path to the file with translations.",
    ),
):
    """
    Read translations from a file and insert them back into the CSV file.
    """
    if not csv_file.exists():
        rprint(f"[red]Error:[/red] CSV file not found: {csv_file}")
        raise typer.Exit(1)
    if not translation_file.exists():
        rprint(f"[red]Error:[/red] Translation file not found: {translation_file}")
        raise typer.Exit(1)

    # Read the CSV file into a list of dictionaries.
    try:
        with csv_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
            if header is None or "Translation" not in header:
                rprint(
                    "[red]Error:[/red] CSV file does not contain the 'Translation' header."
                )
                raise typer.Exit(1)
            rows = list(reader)
    except Exception as e:
        rprint(f"[red]Failed to read CSV:[/red] {e}")
        raise typer.Exit(1)

    # Read translations from the translation file.
    try:
        translations = translation_file.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        rprint(f"[red]Failed to read the translation file:[/red] {e}")
        raise typer.Exit(1)

    if len(rows) != len(translations):
        rprint(
            "[red]Error:[/red] The number of translations does not match the number of rows in the CSV file."
        )
        raise typer.Exit(1)

    # Update each row with the corresponding translation.
    for i, row in enumerate(rows):
        row["Translation"] = translations[i]

    # Write the updated CSV back to the original file.
    try:
        with csv_file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        rprint(f"[red]Failed to write updated CSV:[/red] {e}")
        raise typer.Exit(1)

    rprint(
        f"[green]Success:[/green] CSV file {csv_file} has been updated with new translations."
    )


if __name__ == "__main__":
    app()
