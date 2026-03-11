import numpy as np
import pandas as pd


# Helper Functions
# Safe version to handle the rows with orderbook depth = 0
# As can be seen in EDA, there are over 5000+ rows with minimum depth

def VWPA_safe(bids, asks):
    """
    Compute Volume-Weighted Price Average (VWPA) for bids and asks.
    
    Parameters:
        bids: numpy array of shape (n_levels, 2) or empty
        asks: numpy array of shape (m_levels, 2) or empty
    
    Returns:
        (vwpa_bid, vwpa_ask): floats, np.nan if volume is zero or array is empty
    """
    # Convert to numpy arrays (in case lists are passed)
    bids = np.array(bids, dtype=float)
    asks = np.array(asks, dtype=float)

    # VWPA for bids
    if bids.ndim == 2 and bids.shape[1] == 2 and len(bids) > 0:
        vol_price_bid = np.sum(bids[:,0] * bids[:,1])
        vol_bid = np.sum(bids[:,1])
        vwpa_bid = vol_price_bid / vol_bid if vol_bid != 0 else np.nan
    else:
        vwpa_bid = np.nan

    # VWPA for asks
    if asks.ndim == 2 and asks.shape[1] == 2 and len(asks) > 0:
        vol_price_ask = np.sum(asks[:,0] * asks[:,1])
        vol_ask = np.sum(asks[:,1])
        vwpa_ask = vol_price_ask / vol_ask if vol_ask != 0 else np.nan
    else:
        vwpa_ask = np.nan

    return vwpa_bid, vwpa_ask

def orderbook_imbalance(bids, asks):
    """
    Calculate orderbook imbalance: (vol_bid - vol_ask) / (vol_bid + vol_ask)
    Returns np.nan if total volume = 0.
    """
    # Ensure list inputs are converted before 2D slicing
    bids = np.array(bids, dtype=float)
    asks = np.array(asks, dtype=float)

    vol_bid = np.sum(bids[:,1]) if bids.ndim == 2 and bids.shape[1] == 2 and bids.size > 0 else 0.0
    vol_ask = np.sum(asks[:,1]) if asks.ndim == 2 and asks.shape[1] == 2 and asks.size > 0 else 0.0

    total = vol_bid + vol_ask
    if total == 0:
        return np.nan
    return (vol_bid - vol_ask) / total