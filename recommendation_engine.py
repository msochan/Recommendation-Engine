class RecomendationEngine:
    def __init__(self, sku_name, df, num) -> None:
        self.sku_name = sku_name
        self.df = df
        self.num = num
