import luigi
from src.dataset.html_parser import PipelineOneDay, PipelineOneHour

import datetime

if __name__ == "__main__":
    luigi.build([PipelineOneDay(date=datetime.date(2019, 11, 8)),
                 PipelineOneDay(date=datetime.date(2019, 11, 9)),
                 PipelineOneDay(date=datetime.date(2019, 11, 10)),
                 PipelineOneDay(date=datetime.date(2019, 11, 11)),
                 PipelineOneDay(date=datetime.date(2019, 11, 12)),
                 PipelineOneDay(date=datetime.date(2019, 11, 13)),
                 PipelineOneDay(date=datetime.date(2019, 11, 14)),
                 PipelineOneDay(date=datetime.date(2019, 11, 15)),
                 PipelineOneDay(date=datetime.date(2019, 11, 16)),
                 PipelineOneDay(date=datetime.date(2019, 11, 17))], workers=4)