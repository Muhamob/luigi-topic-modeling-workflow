import os

import luigi
from bs4 import BeautifulSoup

text_tags = ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']
TARGET_DIR = "/home/isabellad/studyspace/luigi-topic-modeling/data/parsed_files/"


class NewsReader(luigi.ExternalTask):
    input_data_dir = luigi.Parameter()
    data = luigi.DateParameter()
    hour = luigi.Parameter()
    news_id = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(str(self.input_data_dir), '{date:%Y%m%d}'.format(date=self.data), f'{self.hour}',
                         f'{self.news_id}.html'))


class MakeFolder(luigi.ExternalTask):
    data = luigi.DateParameter()
    hour = luigi.Parameter()

    def output(self):
        dirname = os.path.join(TARGET_DIR, '{date:%Y%m%d}'.format(date=self.data), f'{self.hour}')
        return luigi.LocalTarget(dirname)

    def run(self):
        dirname = os.path.join(TARGET_DIR, '{date:%Y%m%d}'.format(date=self.data), f'{self.hour}')
        os.makedirs(dirname)


class HTMLParser(luigi.Task):
    input_data_dir = luigi.Parameter()
    data = luigi.DateParameter()
    hour = luigi.Parameter()
    news_id = luigi.IntParameter()

    def requires(self):
        yield MakeFolder(
            data=self.data,
            hour=self.hour
        )
        yield NewsReader(
            input_data_dir=self.input_data_dir,
            data=self.data,
            hour=self.hour,
            news_id=self.news_id
        )

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_DIR, '{date:%Y%m%d}'.format(date=self.data), f'{self.hour}',
                                              str(self.news_id) + '.txt'))

    def run(self):
        with self.input()[1].open('r') as news_file:
            parsed = self.parse_file(news_file)

        with self.output().open('w') as out:
            out.write(parsed)

    @staticmethod
    def parse_file(file):
        soup = BeautifulSoup(file, 'html.parser')
        texts = []
        for tag in text_tags:
            texts += [t.get_text() for t in soup.find_all(tag)]

        return '. '.join(texts)


class PipelineOneHour(luigi.WrapperTask):
    input_data_dir = luigi.Parameter(default='/home/isabellad/studyspace/datasets/news/telegram_contest')
    date = luigi.DateParameter()
    hour = luigi.Parameter()

    def requires(self):
        dir_path = os.path.join(str(self.input_data_dir), '{date:%Y%m%d}'.format(date=self.date), '{:02d}'.format(self.hour))
        ids = [f.replace('.html', '') for f in os.listdir(dir_path)]
        read_tasks = [
            NewsReader(
                input_data_dir=self.input_data_dir,
                data=self.date,
                hour='{:02d}'.format(self.hour),
                news_id=id
            )
            for id in ids
        ]
        make_dir_task = [MakeFolder(data=self.date, hour='{:02d}'.format(self.hour)), ]
        html_parser_tasks = [
            HTMLParser(
                input_data_dir=self.input_data_dir,
                data=self.date,
                hour='{:02d}'.format(self.hour),
                news_id=id
            )
            for id in ids
        ]

        return read_tasks + make_dir_task + html_parser_tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {date} {hour}".format(date=self.date, hour=self.hour))

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_DIR, "PipelineOneHour"))


class PipelineOneDay(luigi.WrapperTask):
    input_data_dir = luigi.Parameter(default='/home/isabellad/studyspace/datasets/news/telegram_contest')
    date = luigi.DateParameter()

    def requires(self):
        hours = list(range(0, 24))

        for hour in hours:
            yield PipelineOneHour(input_data_dir=self.input_data_dir, date=self.date, hour=hour)

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_DIR, "PipelineOneDay"))
