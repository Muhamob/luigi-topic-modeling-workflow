import os

import luigi
from bs4 import BeautifulSoup
from src.dataset.fileReader import NewsReader


text_tags = ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']


def parse_file(file):
    soup = BeautifulSoup(file, 'html.parser')
    texts = []
    for tag in text_tags:
        texts += [t.get_text() for t in soup.find_all(tag)]

    return '. '.join(texts)


class MakeFolder(luigi.ExternalTask):
    dirname = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dirname)

    def run(self):
        os.mkdir(self.dirname)


class HTMLParser(luigi.Task):
    raw_data_dir = '/home/isabellad/studyspace/datasets/news/telegram_contest/20191108/'
    out_data_dir = '/home/isabellad/studyspace/luigi-topic-modeling/data/parsed_files/'
    filename = luigi.Parameter()

    def requires(self):
        yield MakeFolder(self.out_data_dir)
        yield NewsReader(os.path.join(self.raw_data_dir, str(self.filename)))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_data_dir, str(self.filename)))

    def run(self):
        with self.input().open('r') as news_file:
            parsed = parse_file(news_file)

        with self.output().open('w') as out:
            out.write(parsed)
