"""
Main file to run all the programs.
"""
import os, sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

# Importing the scripts to generate the urls
import scripts.generate_url as gu
# Importing the scripts to scrape email information
import scraping.BussWithNoEml as bu
# Importing the scripts to scrape phone information
import scraping.BussWithNoPhone as nu


def main():
    print("\n=== Starting The Program ===")
    gu.main()
    bu.main()
    nu.main()
    print("\n=== Ending The Program ===")


if __name__ == "__main__":
    main()

