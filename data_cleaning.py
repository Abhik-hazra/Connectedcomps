import h2o
import numpy as np
import pandas as pd

def calculate_iv_woe(h2o_df, target, bins=5, zero_bin=False):
    """
    Compute WOE/IV for each numeric feature in `h2o_df` against binary `target`.
      - bins: total number of buckets to create (including zero‐bucket if zero_bin=True)
      - zero_bin: if True, all exactly‐zero values go into one bucket and the rest are split into (bins-1) quantile buckets.
    """
    # ensure target is numeric so sum() works for bad‐count
    if h2o_df[target].isfactor():
        h2o_df[target] = h2o_df[target].asnumeric()

    # pick only numeric columns (and skip the target itself):
    numeric_cols = [
        c for c,d in h2o_df.types.items()
        if d in ("int","real") and c != target
    ]

    iv_summary = {}
    woe_dict_all = {}

    for feature in numeric_cols:
        try:
            # deep copy so we never clobber the original
            h2o_df_cp = h2o.deep_copy(h2o_df, 'h2o_df_cp')

            # how many splits for non-zero values?
            if zero_bin:
                # one bucket for zeros, so non-zero get (bins-1) buckets
                split_count = bins - 1
            else:
                split_count = bins

            # if the feature has very few uniques, treat as categorical
            if h2o_df_cp[feature].nunique()[0] <= split_count:
                binned_col = feature + '_binned'
                h2o_df_cp[binned_col] = h2o_df_cp[feature].asfactor()
            else:
                # build your list of quantile‐probabilities
                probs = [i/float(split_count) for i in range(1, split_count)]
                # optionally compute quantiles on non-zero only
                if zero_bin:
                    nonzero = h2o_df_cp[h2o_df_cp[feature] != 0]
                    quantiles = nonzero[feature].quantile(probs)
                    min_val = float(nonzero[feature].min())
                    max_val = float(nonzero[feature].max())
                else:
                    quantiles = h2o_df_cp[feature].quantile(probs)
                    min_val = float(h2o_df_cp[feature].min())
                    max_val = float(h2o_df_cp[feature].max())

                # unique, sorted break‐points
                qvals = sorted(set(quantiles.as_data_frame(use_pandas=True).iloc[:,0].astype(float)))
                breaks = [min_val] + qvals + [max_val]

                binned_col = feature + '_binned'

                if zero_bin:
                    # cut the non-zero portion
                    cut_nonzero = h2o_df_cp[feature]\
                                    .cut(breaks=breaks, include_lowest=True)\
                                    .ascharacter()
                    # build a new column: zeros→"Zero", else→the cut‐bucket
                    h2o_df_cp[binned_col] = h2o_df_cp[feature]\
                        .ifelse(h2o_df_cp[feature] == 0,
                                "Zero",
                                cut_nonzero)\
                        .asfactor()
                else:
                    h2o_df_cp[binned_col] = h2o_df_cp[feature]\
                                              .cut(breaks=breaks, include_lowest=True)\
                                              .asfactor()

            #-------------------------------------------------------
            # now do the same bad/total‐count → WOE, IV calculation
            #-------------------------------------------------------
            # total counts per bin
            grp = h2o_df_cp[binned_col].table()
            grp.set_names(['bin','total'])
            # bad counts per bin
            bad = h2o_df_cp.group_by(binned_col).sum(target).get_frame()
            bad.set_names(['bin','bad'])

            # move to pandas to finish the math
            df_tot = grp.as_data_frame(use_pandas=True)
            df_bad = bad.as_data_frame(use_pandas=True)
            df = df_tot.merge(df_bad, on='bin', how='left')
            df['bad'] = df['bad'].fillna(0)
            df['good'] = df['total'] - df['bad']

            # avoid divide‐0
            df['bad']  = df['bad'].replace(0, 0.5)
            df['good'] = df['good'].replace(0, 0.5)

            # distributions
            df['bad_dist']  = df['bad']  / df['bad'].sum()
            df['good_dist'] = df['good'] / df['good'].sum()

            # WOE & IV
            df['woe'] = np.log(df['good_dist'] / df['bad_dist'])
            df['iv']  = (df['good_dist'] - df['bad_dist']) * df['woe']

            iv_summary[feature]   = df['iv'].sum()
            woe_dict_all[feature] = dict(zip(df['bin'], df['woe']))

        except Exception as e:
            print(f"Skipping {feature} due to error: {e}")

    iv_df = pd.DataFrame.from_dict(
        iv_summary, orient='index', columns=['iv']
    ).reset_index().rename(columns={'index':'feature'}).sort_values('iv', ascending=False)

    return iv_df, woe_dict_all
