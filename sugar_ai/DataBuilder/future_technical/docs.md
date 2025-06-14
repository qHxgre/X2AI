# 期货技术指标因子数据

构建期货技术指标因子数据，是量化分析和交易策略开发中的重要步骤。以下是最全面的技术指标汇总，覆盖了趋势类、动量类、震荡类、成交量类和价格类等指标，每个指标包含释义、用法、公式。

---

## 一、趋势类指标（用于判断行情趋势）

### 1. MA（Moving Average，移动平均线）
- **释义**：一定周期内价格的平均值，分为简单（SMA）、加权（WMA）、指数（EMA）等。  
- **用法**：判断趋势方向，均线金叉/死叉作为买卖信号。  
- **公式**：  
  - SMA(N) = (P₁ + P₂ + … + Pₙ) / N  
  - EMA(N) = EMA(yesterday) × (1 − α) + P(today) × α，其中 α = 2 / (N+1)  

---

### 2. MACD（Moving Average Convergence Divergence）
- **释义**：EMA的差值用于判断多空力量。  
- **用法**：DIF上穿DEA为金叉，买入信号；下穿为死叉。  
- **公式**：  
  - DIF = EMA(12) − EMA(26)  
  - DEA = EMA(DIF, 9)  
  - MACD柱 = 2 × (DIF − DEA)  

---

### 3. DMI（Directional Movement Index，动向指标）
- **释义**：评估市场趋势强度。  
- **用法**：+DI上穿−DI为买入信号；ADX用于判断趋势强弱。  
- **公式**：  
  - +DI = 100 × EMA(+DM/TR, N)  
  - −DI = 100 × EMA(−DM/TR, N)  
  - ADX = EMA(|+DI − −DI| / (+DI + −DI), N)  

---

## 二、动量类指标（用于判断价格动能）

### 4. RSI（Relative Strength Index，相对强弱指标）
- **释义**：衡量上涨/下跌动能的强弱。  
- **用法**：RSI > 70 超买，< 30 超卖。  
- **公式**：  
  - RSI = 100 − 100 / (1 + RS)，其中 RS = Avg(up close) / Avg(down close)  

---

### 5. CCI（Commodity Channel Index，顺势指标）
- **释义**：价格偏离统计平均值的程度。  
- **用法**：CCI > 100 为超买；< −100 为超卖。  
- **公式**：  
  - TP = (High + Low + Close) / 3  
  - CCI = (TP − MA(TP, N)) / (0.015 × Mean Deviation)  

---

### 6. ROC（Rate of Change，变动率指标）
- **释义**：当前价格相对于若干期前的变动百分比。  
- **用法**：识别转势点。  
- **公式**：ROC = (Close − CloseN) / CloseN × 100%  

---

## 三、震荡类指标（用于判断市场是否过热或过冷）

### 7. KD指标（随机指标，KDJ）
- **释义**：分析价格在一定周期的相对位置。  
- **用法**：K线上穿D线为买入信号。  
- **公式**：  
  - RSV = (Close − Lowest Low) / (Highest High − Lowest Low) × 100  
  - K = 2/3 × K₁ + 1/3 × RSV  
  - D = 2/3 × D₁ + 1/3 × K  
  - J = 3 × K − 2 × D  

---

### 8. BOLL（Bollinger Bands，布林带）
- **释义**：价格围绕移动平均波动的统计通道。  
- **用法**：价格接触上轨为压力，下轨为支撑。  
- **公式**：  
  - 中轨 = MA(N)  
  - 上轨 = MA(N) + K × 标准差  
  - 下轨 = MA(N) − K × 标准差  

---

### 9. ATR（Average True Range，平均真实波幅）
- **释义**：衡量价格波动强度。  
- **用法**：用于设定止损/追踪止盈。  
- **公式**：  
  - TR = max{High − Low, |High − Close₁|, |Low − Close₁|}  
  - ATR = EMA(TR, N)  

---

## 四、成交量类指标（衡量市场参与度）

### 10. OBV（On Balance Volume，能量潮）
- **释义**：价格与成交量结合评估趋势。  
- **用法**：OBV上升表示资金流入。  
- **公式**：  
  - 若Close > Close₁：OBV = OBV₁ + Vol  
  - 若Close < Close₁：OBV = OBV₁ − Vol  
  - 否则 OBV = OBV₁  

---

### 11. VOL指标（成交量指标）
- **释义**：衡量市场活跃程度。  
- **用法**：量价配合判断突破有效性。  
- **公式**：无固定公式，常配合均量线使用（如MA(Volume, 5)）  

---

### 12. MFI（Money Flow Index，资金流量指标）
- **释义**：价格与成交量结合的RSI版本。  
- **用法**：MFI > 80 超买；< 20 超卖。  
- **公式**：  
  - TP = (High + Low + Close)/3  
  - Raw Money Flow = TP × Volume  
  - MFI = 100 − 100 / (1 + 正流/负流)  

---

## 五、价格行为类指标

### 13. PSY（心理线指标）
- **释义**：分析一段时间内上涨天数占比。  
- **用法**：反映市场情绪，>75 过热，<25 过冷。  
- **公式**：PSY = N日中上涨日数 / N × 100%  

---

### 14. WR（Williams %R 威廉指标）
- **释义**：收盘价在区间高低位置的相对强弱。  
- **用法**：WR < −80 超卖，WR > −20 超买。  
- **公式**：  
  - WR = (Highest High − Close) / (Highest High − Lowest Low) × (−100)  

---

### 15. SAR（Stop And Reverse，抛物转向）
- **释义**：趋势反转信号，类似追踪止损线。  
- **用法**：用于设定止盈止损。  
- **公式**：较复杂，基于极值和加速因子递推计算。  