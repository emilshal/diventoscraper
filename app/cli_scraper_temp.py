import argparse
from pathlib import Path
from app.core.temp_scraper import scrape_destinations_temp


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Divento temporary exhibitions scraper on a list of cities from a file"
    )
    parser.add_argument(
        "input_file",
        help="Path to a text or CSV file containing one city per line",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=24,
        help="Number of months from today to search (default: 24)",
    )
    parser.add_argument(
        "--output",
        help="Optional path for the resulting Excel file. If omitted the default"
        " location from config is used.",
    )
    args = parser.parse_args()

    with open(args.input_file, "r", encoding="utf-8") as fh:
        cities = [l.strip() for l in fh if l.strip()]

    out_path = scrape_destinations_temp(cities, months=args.months)

    if args.output:
        out_file = Path(args.output)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).replace(out_file)
        out_path = str(out_file)

    print(out_path)


if __name__ == "__main__":
    main()
