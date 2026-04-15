from stock_oracle.collectors.yahoo_finance import YahooFinanceCollector
from stock_oracle.collectors.reddit_sentiment import RedditSentimentCollector
from stock_oracle.collectors.sec_edgar import SECEdgarCollector
from stock_oracle.collectors.job_postings import JobPostingsCollector, get_company_name
from stock_oracle.collectors.advanced_signals import (
    SupplyChainCollector,
    GovernmentContractsCollector,
    PatentActivityCollector,
    CongressionalTradesCollector,
)
from stock_oracle.collectors.alt_data import (
    AppStoreCollector,
    SeasonalityCollector,
    WeatherCorrelationCollector,
    NewsSentimentCollector,
    ShippingActivityCollector,
    DomainRegistrationCollector,
    EarningsCallNLPCollector,
    EmployeeSentimentCollector,
)
from stock_oracle.collectors.creative_signals import (
    WaffleHouseIndexCollector,
    GitHubVelocityCollector,
    GoogleTrendsCollector,
    CardboardIndexCollector,
    WikipediaVelocityCollector,
    EnergyCascadeCollector,
    HackerNewsSentimentCollector,
    TalentFlowCollector,
)
from stock_oracle.collectors.cross_stock import (
    CrossStockCollector,
    EarningsContagionCollector,
)
from stock_oracle.collectors.finnhub_collector import FinnhubCollector
