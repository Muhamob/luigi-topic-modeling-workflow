import luigi

from src.dataset.html_parser import HTMLParser

if __name__ == "__main__":
    luigi.build([HTMLParser('00/993066303998339.html')], )