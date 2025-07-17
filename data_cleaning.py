import pandas as pd

def score_binning_summary(h2o_df, target_col, score_col, num_bins=10):
    """
    Bins the score_col into quantiles (with zeros as a separate bin), 
    and calculates summary statistics by bin for a binary target in an H2OFrame.

    Args:
        h2o_df: H2OFrame input containing at least target_col and score_col.
        target_col: Name of the binary target column (e.g., 'fraud').
        score_col: Name of the score column (e.g., 'num_pgrnk_score_nb').
        num_bins: Number of quantile bins for non-zero scores (default 10).

    Returns:
        Pandas DataFrame with bin summary statistics.
    """
    # Convert to pandas DataFrame
    data = h2o_df[[target_col, score_col]].as_data_frame(use_pandas=True)

    # Bin zero/nonzero
    zero_mask = data[score_col] == 0
    nonzero_mask = ~zero_mask

    # Assign 'zero' bin label
    data['Quantile_Bin'] = None
    data.loc[zero_mask, 'Quantile_Bin'] = -1  # -1 for zeros

    # Bin non-zero into quantiles
    if nonzero_mask.sum() > 0:
        data.loc[nonzero_mask, 'Quantile_Bin'] = pd.qcut(
            data.loc[nonzero_mask, score_col],
            q=num_bins, labels=False, duplicates='drop'
        )
    data['Quantile_Bin'] = data['Quantile_Bin'].astype(int)
    
    # Amount column (example, optional)
    data['total_good_amount'] = (1 - data[target_col])

    # Group by bin
    vals = data.groupby('Quantile_Bin').agg(
        nrow=(target_col, 'count'),
        fraud=(target_col, 'sum'),
        non_fraud=(lambda x: (1 - x).sum())
    ).reset_index()
    
    # Cumulative metrics (sort so zero bin comes first)
    vals = vals.sort_values('Quantile_Bin', ascending=True)
    vals['cum_fraud'] = vals['fraud'].cumsum()
    vals['cum_non_fraud'] = vals['non_fraud'].cumsum()

    # Evaluation metrics
    total_fraud = vals['fraud'].sum()
    total_non_fraud = vals['non_fraud'].sum()
    vals['Detection_Rate'] = (vals['cum_fraud'] / total_fraud * 100).round(2)
    vals['False_Positive_Rate'] = (vals['cum_non_fraud'] / total_non_fraud * 100).round(2)
    vals['Row_Rate'] = (vals['nrow'] / vals['nrow'].sum() * 100).round(2)

    # Record min/max score for each bin (except zeros)
    if nonzero_mask.sum() > 0:
        score_bins = data.loc[nonzero_mask].groupby('Quantile_Bin')[score_col].agg(['min', 'max']).reset_index()
        vals = pd.merge(vals, score_bins, on='Quantile_Bin', how='left')
        # Fill zero bin mins/max with zero
        vals.loc[vals['Quantile_Bin'] == -1, ['min', 'max']] = 0

    return vals
