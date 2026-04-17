import sys
import time
import pandas as pd
import numpy as np
import yfinance as yf
from typing import List, Dict, Tuple, Optional

# Optional nselib import
try:
    from nselib import capital_market
    NSELIB_AVAILABLE = True
except ImportError:
    NSELIB_AVAILABLE = False
    print("Note: nselib not installed. Using fallback F&O stock list.")

# Configuration
ROUND_PRECI = 2
LOOKBACK_WEEKS = 12
THRESHOLD = 0.01  # 1% tolerance for price to level proximity
ODD_SQUARE_TOLERANCE = 1.0  # 1% tolerance for level to odd square
PRICE_TOLERANCE = 1.0  # 1% tolerance for current price to level

# Fallback F&O stocks
FALLBACK_FNO_STOCKS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", 
    "SBIN", "LT", "HINDUNILVR", "BHARTIARTL", "KOTAKBANK",
    "ITC", "AXISBANK", "BAJFINANCE", "WIPRO", "TATAMOTORS"
]


def generate_odd_squares(limit: int = 200) -> List[Tuple[int, int]]:
    """Generate squares of odd numbers up to a given limit."""
    odd_squares = []
    for num in range(1, limit + 1, 2):
        odd_squares.append((num, num * num))
    return odd_squares


def is_price_within_tolerance(price: float, target: float, percent: float = 1.0) -> bool:
    """Check if a price is within a given percentage of a target value."""
    if price <= 0 or target <= 0:
        return False
    lower_bound = target * (1 - percent / 100)
    upper_bound = target * (1 + percent / 100)
    return lower_bound <= price <= upper_bound


def find_nearest_odd_square(price: float, odd_squares: List[Tuple[int, int]]) -> Optional[Tuple[int, int, float]]:
    """Find the odd square closest to the given price."""
    if price <= 0:
        return None
    
    min_distance = float('inf')
    closest_square = None
    closest_odd = None
    
    for odd_num, square in odd_squares:
        distance = abs(price - square)
        if distance < min_distance:
            min_distance = distance
            closest_square = square
            closest_odd = odd_num
    
    if closest_square:
        deviation = abs(price - closest_square) / closest_square * 100
        return (closest_odd, closest_square, deviation)
    return None


def calculate_volatility_levels(df: pd.DataFrame) -> Optional[Tuple[float, float, float, float]]:
    """
    Calculate volatility-based buy/sell levels.
    Returns: (buy_above, buy_confirm, sell_below, sell_confirm)
    """
    df = df.dropna()
    
    if len(df) < LOOKBACK_WEEKS + 2:
        return None
    
    closes = df['Close'].astype(float).values
    opens = df['Open'].astype(float).values
    
    dc = float(closes[-2])
    dc_list = closes[-(LOOKBACK_WEEKS+1):-1]
    
    # Log returns and volatility
    log_returns = np.log(dc_list[1:] / dc_list[:-1])
    volatility = float(np.sqrt(np.var(log_returns)))
    
    dop = float(opens[-1])
    range1 = round(float(dc * volatility), ROUND_PRECI)
    
    # Gap adjustment
    doKdc = dop if abs(dop - dc) > (0.382 * range1) else dc
    
    # Recalculate volatility
    adj = np.append(dc_list[:-1], doKdc)
    log_returns = np.log(adj[1:] / adj[:-1])
    volatility = float(np.sqrt(np.var(log_returns)))
    
    range1 = round(float(doKdc * volatility), ROUND_PRECI)
    
    # Calculate levels
    buy_above = round(float(doKdc + (range1 * 0.236)), ROUND_PRECI)
    buy_confirm = round(float(doKdc + (range1 * 0.382)), ROUND_PRECI)
    sell_below = round(float(doKdc - (range1 * 0.236)), ROUND_PRECI)
    sell_confirm = round(float(doKdc - (range1 * 0.382)), ROUND_PRECI)
    
    return buy_above, buy_confirm, sell_below, sell_confirm


def is_near_level(price: float, level: float, threshold: float = THRESHOLD) -> bool:
    """Check if price is within threshold% of a given level."""
    if level <= 0:
        return False
    return abs(price - level) / level <= threshold


def get_fno_stocks() -> List[str]:
    """Get list of F&O stocks from NSE."""
    if NSELIB_AVAILABLE:
        try:
            fno_data = capital_market.fno_equity_list()
            if hasattr(fno_data, 'empty') and not fno_data.empty:
                if 'SYMBOL' in fno_data.columns:
                    return fno_data['SYMBOL'].tolist()
                elif 'symbol' in fno_data.columns:
                    return fno_data['symbol'].tolist()
                else:
                    return fno_data.iloc[:, 0].tolist()
        except Exception as e:
            print(f"Error fetching F&O list: {e}")
    
    print(f"Using fallback list of {len(FALLBACK_FNO_STOCKS)} common F&O stocks...")
    return FALLBACK_FNO_STOCKS


def get_stock_data(symbols: List[str]) -> Dict[str, Dict]:
    """
    Fetch comprehensive stock data including current price and historical data.
    Returns: Dict with symbol as key, containing price, df, and levels info.
    """
    stock_data = {}
    total = len(symbols)
    
    print(f"\nFetching data for {total} stocks...")
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"  Processing {symbol} ({idx}/{total})...", end=" ", flush=True)
        
        try:
            yf_symbol = f"{symbol}.NS"
            ticker = yf.Ticker(yf_symbol)
            
            # Get historical data for volatility calculation
            df = yf.download(yf_symbol, period="6mo", interval="1wk", progress=False)
            
            if df.empty:
                print("✗ No historical data")
                continue
            
            # Get current price
            hist = ticker.history(period="1d")
            if not hist.empty and 'Close' in hist.columns:
                current_price = float(hist['Close'].iloc[-1])
            else:
                info = ticker.info
                if isinstance(info, dict):
                    current_price = (info.get('regularMarketPrice') or 
                                   info.get('currentPrice') or 
                                   info.get('previousClose'))
                    if current_price:
                        current_price = float(current_price)
                    else:
                        current_price = float(df['Close'].iloc[-1])
                else:
                    current_price = float(df['Close'].iloc[-1])
            
            if not current_price or current_price <= 0:
                print("✗ Invalid price")
                continue
            
            # Calculate volatility levels
            levels = calculate_volatility_levels(df)
            
            stock_data[symbol] = {
                'price': current_price,
                'df': df,
                'levels': levels,
                'has_levels': levels is not None
            }
            
            status = "✓" if levels else "○"
            print(f"{status} ₹{current_price:.2f}")
            
        except Exception as e:
            print(f"✗ Error: {str(e)[:30]}")
        
        time.sleep(0.1)  # Rate limiting
    
    return stock_data


def find_stocks_with_level_near_odd_square(stock_data: Dict, 
                                          odd_tolerance: float = 1.0,
                                          price_tolerance: float = 1.0) -> Dict:
    """
    Find stocks where:
    1. ANY volatility level (Buy Above, Buy Confirm, Sell Below, Sell Confirm) 
       is within odd_tolerance% of an odd square
    2. Current price is within price_tolerance% of that same volatility level
    """
    odd_squares = generate_odd_squares(limit=200)
    matches = {}
    
    for symbol, data in stock_data.items():
        if not data['has_levels']:
            continue
        
        current_price = data['price']
        buy_above, buy_confirm, sell_below, sell_confirm = data['levels']
        
        # Define all levels with their names
        levels_list = [
            ('BUY_ABOVE', buy_above),
            ('BUY_CONFIRM', buy_confirm),
            ('SELL_BELOW', sell_below),
            ('SELL_CONFIRM', sell_confirm)
        ]
        
        # Check each level
        for level_name, level_value in levels_list:
            if level_value <= 0:
                continue
            
            # Condition 1: Is this level near an odd square?
            odd_match = None
            for odd_num, square in odd_squares:
                if is_price_within_tolerance(level_value, square, odd_tolerance):
                    deviation_level_to_square = abs(level_value - square) / square * 100
                    odd_match = {
                        'odd_number': odd_num,
                        'odd_square': square,
                        'deviation_level_to_square': round(deviation_level_to_square, 2)
                    }
                    break
            
            if not odd_match:
                continue
            
            # Condition 2: Is current price near this level?
            if not is_near_level(current_price, level_value, price_tolerance):
                continue
            
            # Both conditions met!
            deviation_price_to_level = abs(current_price - level_value) / level_value * 100
            
            if symbol not in matches:
                matches[symbol] = {
                    'price': current_price,
                    'levels': data['levels'],
                    'matched_levels': []
                }
            
            matches[symbol]['matched_levels'].append({
                'level_name': level_name,
                'level_value': level_value,
                'odd_info': odd_match,
                'deviation_price_to_level': round(deviation_price_to_level, 2)
            })
    
    # Sort matched levels within each stock by deviation (best match first)
    for symbol in matches:
        matches[symbol]['matched_levels'].sort(key=lambda x: x['deviation_price_to_level'])
    
    return matches


def main():
    print("=" * 80)
    print("ADVANCED STOCK SCREENER - NSE F&O Segment")
    print("FILTER CRITERIA:")
    print("  1. Volatility level (Buy Above/Confirm OR Sell Below/Confirm) is within 1% of Odd Square")
    print("  2. Current price is within 1% of that same volatility level")
    print("=" * 80)
    
    # Get F&O stock list
    print("\n[1/4] Fetching F&O stock list...")
    fno_symbols = get_fno_stocks()
    print(f"Found {len(fno_symbols)} stocks in F&O segment.")
    
    # Fetch stock data
    print("\n[2/4] Fetching price data and calculating levels...")
    stock_data = get_stock_data(fno_symbols)
    
    if not stock_data:
        print("\n❌ No stock data retrieved. Exiting.")
        sys.exit(0)
    
    print(f"\n✅ Retrieved data for {len(stock_data)}/{len(fno_symbols)} stocks")
    
    # Find stocks meeting the criteria
    print("\n[3/4] Filtering stocks...")
    print("  - Checking if any volatility level is within 1% of odd square")
    print("  - Checking if current price is within 1% of that level")
    
    filtered_stocks = find_stocks_with_level_near_odd_square(stock_data, 
                                                            odd_tolerance=1.0,
                                                            price_tolerance=1.0)
    
    print(f"\n✅ Found {len(filtered_stocks)} stock(s) meeting ALL criteria")
    
    # Display results
    print("\n" + "=" * 80)
    print("FILTERED RESULTS - Stocks Meeting Both Conditions")
    print("=" * 80)
    
    if filtered_stocks:
        print(f"\n{'Symbol':<15} {'Price':<12} {'Level Name':<15} {'Level Value':<12} "
              f"{'Odd Square':<18} {'Price Dev':<10}")
        print("-" * 95)
        
        for symbol, info in filtered_stocks.items():
            # Show best match (lowest price deviation)
            best_match = info['matched_levels'][0]
            print(f"{symbol:<15} ₹{info['price']:<10.2f} "
                  f"{best_match['level_name']:<15} ₹{best_match['level_value']:<10.2f} "
                  f"{best_match['odd_info']['odd_number']}²={best_match['odd_info']['odd_square']:<8} "
                  f"{best_match['deviation_price_to_level']:.2f}%")
            
            # Show additional matches if any
            for match in info['matched_levels'][1:]:
                print(f"{'':<15} {'':<12} {match['level_name']:<15} ₹{match['level_value']:<10.2f} "
                      f"{match['odd_info']['odd_number']}²={match['odd_info']['odd_square']:<8} "
                      f"{match['deviation_price_to_level']:.2f}%")
        
        # Detailed analysis for each stock
        print("\n" + "=" * 80)
        print("DETAILED ANALYSIS FOR EACH FILTERED STOCK")
        print("=" * 80)
        
        for symbol, info in filtered_stocks.items():
            print(f"\n📊 {symbol}")
            print(f"   Current Price: ₹{info['price']:.2f}")
            print(f"\n   📈 VOLATILITY LEVELS:")
            ba, bc, sb, sc = info['levels']
            print(f"      Buy Above:     ₹{ba:.2f}")
            print(f"      Buy Confirm:   ₹{bc:.2f}")
            print(f"      Sell Below:    ₹{sb:.2f}")
            print(f"      Sell Confirm:  ₹{sc:.2f}")
            
            print(f"\n   ✅ MATCHED CRITERIA:")
            for match in info['matched_levels']:
                odd_info = match['odd_info']
                print(f"\n      🎯 {match['level_name']} = ₹{match['level_value']:.2f}")
                print(f"         → Within {odd_info['deviation_level_to_square']:.2f}% of Odd Square: "
                      f"{odd_info['odd_number']}² = {odd_info['odd_square']}")
                print(f"         → Current price ₹{info['price']:.2f} is within "
                      f"{match['deviation_price_to_level']:.2f}% of this level")
            
            # Trading suggestion
            best_match = info['matched_levels'][0]
            print(f"\n   💡 TRADING INSIGHT:")
            if 'BUY' in best_match['level_name']:
                print(f"      ✓ Strong BUY signal detected")
                print(f"      ✓ Entry near ₹{best_match['level_value']:.2f}")
                print(f"      ✓ Current price ₹{info['price']:.2f} is ideal for entry")
                print(f"      ✓ Stop Loss: Below ₹{sc:.2f} (Sell Confirm)")
                print(f"      ✓ Target 1: ₹{bc + (bc-ba):.2f}")
                print(f"      ✓ Target 2: ₹{ba + 2*(bc-ba):.2f}")
            else:
                print(f"      ✓ Strong SELL signal detected")
                print(f"      ✓ Entry near ₹{best_match['level_value']:.2f}")
                print(f"      ✓ Current price ₹{info['price']:.2f} is ideal for entry")
                print(f"      ✓ Stop Loss: Above ₹{ba:.2f} (Buy Above)")
                print(f"      ✓ Target 1: ₹{sb - (sb-sc):.2f}")
                print(f"      ✓ Target 2: ₹{sc - 2*(sb-sc):.2f}")
            
            # Gann significance
            odd_info = best_match['odd_info']
            print(f"\n   🔢 GANN SIGNIFICANCE:")
            print(f"      {odd_info['odd_number']} is an odd number")
            print(f"      Square = {odd_info['odd_square']}")
            print(f"      This level aligns with Gann Square of Nine theory")
            
            print("-" * 50)
    
    else:
        print("\n❌ No stocks found meeting ALL criteria:")
        print("   - Volatility level within 1% of odd square")
        print("   - Current price within 1% of that level")
        print("\n💡 Suggestions:")
        print("   - Increase tolerance levels (e.g., to 2%)")
        print("   - Check during market hours for real-time prices")
        print("   - Expand odd square range beyond 200")
        print("   - Run during high volatility periods")
    
    # Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total F&O stocks analyzed: {len(fno_symbols)}")
    print(f"Stocks with valid data: {len(stock_data)}")
    print(f"Stocks meeting ALL criteria: {len(filtered_stocks)}")
    
    if filtered_stocks:
        total_opportunities = sum(len(info['matched_levels']) for info in filtered_stocks.values())
        print(f"Total trading opportunities found: {total_opportunities}")
        
        # Categorize by signal type
        buy_signals = 0
        sell_signals = 0
        for info in filtered_stocks.values():
            for match in info['matched_levels']:
                if 'BUY' in match['level_name']:
                    buy_signals += 1
                else:
                    sell_signals += 1
        print(f"   - BUY signals: {buy_signals}")
        print(f"   - SELL signals: {sell_signals}")
    
    if stock_data:
        prices = [d['price'] for d in stock_data.values()]
        print(f"\nPrice Range of all stocks: ₹{min(prices):.2f} - ₹{max(prices):.2f}")
        print(f"Average Price: ₹{sum(prices)/len(prices):.2f}")
        
        if filtered_stocks:
            filtered_prices = [info['price'] for info in filtered_stocks.values()]
            print(f"\nFiltered stocks price range: ₹{min(filtered_prices):.2f} - ₹{max(filtered_prices):.2f}")
            print(f"Average filtered price: ₹{sum(filtered_prices)/len(filtered_prices):.2f}")
    
    print("\n" + "=" * 80)
    print("FILTER LOGIC:")
    print("  For each stock, we check all 4 volatility levels:")
    print("    BUY_ABOVE, BUY_CONFIRM, SELL_BELOW, SELL_CONFIRM")
    print("  ")
    print("  A stock is selected if:")
    print("    1. Level value is within 1% of any odd square (1, 9, 25, 49, 81, 121, 169...)")
    print("    2. Current price is within 1% of that same level")
    print("=" * 80)


if __name__ == "__main__":
    main()
