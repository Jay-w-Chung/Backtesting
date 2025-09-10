import pandas as pd


class Trend():
    # bollinger bands
    def BB_calculation(self, prices_df, cd, n=20, sigma=2):
        s = prices_df[cd]

        rolling_mean = s.rolling(n, min_periods=n)
        center = rolling_mean.mean()    
        std = rolling_mean.std()

        ub = center + sigma * std
        lb = center - sigma * std

        bb = pd.DataFrame({
            cd: s,
            'center': center, 
            'ub': ub,
            'lb': lb,
            'pct_b': (s- lb) / (ub - lb),
            'band_size': (ub - lb) / center * 1000,
            'volume': prices_df['volume']
        })

        #previous values and change rates
        bb['band_size'].replace(0, 0.0001, inplace=True)
        bb['band_size_prev'] = bb['band_size'].shift()
        bb['volume_prev'] = bb['volume'].shift()
        bb['size_chg'] = (bb['band_size'].pct_change() * 100).round(2)
        bb['volume_chg'] = (bb['volume'].pct_change() * 100).round(2)

        return bb.ffill()


    #moving average
    def MA(self, df, cd, long=20, short=5):
        return pd.DataFrame({
            cd: df[cd],
            'MA short': df[cd].rolling(short, min_periods=short).mean(),
            'MA long': df[cd].rolling(long, min_periods=long).mean()
        })
    
    #RSI (vectorised)
    def RSI(self, df, cd, period=5): #default = 14 periods
        diff = df[cd].diff()
        gain = diff.clip(lower=0)
        loss = -diff.clip(upper=0)

        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return pd.DataFrame({cd: df[cd], 'RSI': rsi})

 
    #WRSI
    def WRSI(self, df, cd, period=20):
        diff = df[cd].diff().fillna(0)

        def weighted_rsi(x):
            weights = np.arrange(len(x), 0, -1) / len(x)
            gains = (x.clip(lower = 0) * weights).sum()
            losses = (-x.clip(upper = 0) * weights).sum()
            return 100 * gains / (gains + losses) if (gains + losses) != 0 else 0

        wris = diff.rolling(window=period).apply(weighted_rsi, raw=True)

        rsi_df = pd.DataFrame({cd :df[cd], 'WRSI': wrsi})
        rsi_df['WRSI_diff'] = rsi_df['WRIS'].diff()
        return rsi_df


    #MACD (EMA-based)
    def MACD(self, df, cd, long=26, short=12, signal=9):
        short_ema = df[cd].ewm(span=short, adjust=False).mean()
        long_ema = df[cd].ewm(span=long, adjust=False).mean()
        macd_line = short_ema - long_ema
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        hist = macd_line - signal_line

        return pd.DataFrame({
            cd: df[cd],
            'MACD': macd_line,
            'Signal': signal_line,
            'Hist': hist
        })

    # Stochastic Oscillator (vectorised)
    def stochastic_osc(self, df, cd, period_n=5, period_m=3, period_t=3):
        low_min = df[cd].rolling(window=period_n, min_periods=period_n).min()
        high_max = df[cd].rolling(window=period_n, min_periods=period_n).max()

        fast_k = 100 * (df[cd] - low_min) / (high_max - low_min)
        fast_d = fast_k.rolling(window=period_m, min_periods=period_m).mean() #MA of fast %k
        slow_k = fast_k.rolling(window=period_t, min_periods=period_t).mean() #MA of fast %k over period_t
        slow_d = fast_d.rolling(window=period_t, min_periods=period_t).mean() #MA of fast %d over period_t

        return pd.DataFrame({
            cd: df[cd],
            'fast_k': fast_k,
            'fast_d': fast_d,
            'slow_k': slow_k,
            'slow_d': slow_d
        })

    # fast %k: the most sensitive line, directly reflects the current price's relative position
    # fast %d: a MA of fast %k, reduces noise
    # slow %k, slow %d: further smoothed version, reliable for confirming trends
    #slow_k > slow_d -> buy signal
    #slow_k < slow_d -> sell signal