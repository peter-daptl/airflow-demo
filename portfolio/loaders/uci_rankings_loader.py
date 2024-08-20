from portfolio.loaders.base_loader import BaseLoader


class UCIRankingsLoader(BaseLoader):

    FK_PRIORITY = ["country", "team", "rider"]
