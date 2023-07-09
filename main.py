import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas._libs.lib import Decimal
pd.options.mode.chained_assignment = None  # default='warn'


# date/time of transaction
# number of shares
# price per share transaction
# wheather its buy or sell
# ticker

# Access Data From DataFrame In Python
# https://www.c-sharpcorner.com/article/access-data-from-dataframe/

# Website to download currency exchange rate csv (Note: Some days are not recorded have to improvise by offset rates)
# https://au.investing.com/currencies/usd-aud-historical-data

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    df_transactions = pd.read_csv('S20-22.csv')
    # df_transactions = pd.read_csv('20_21 - Copy.csv')
    currency_conversions = pd.read_csv('USD_AUD Historical Data.csv')

    df_transactions = cleanup(df_transactions)  # sets datatype for columns
    convert_prices_to_aud(df_transactions, currency_conversions)  # convert usd prices to aud
    subset_pairs = df_transactions.groupby('Pair').apply(dict)  # split all by pair as a dictionary
    dict_subset = format_into_df(subset_pairs)  # convert all the dictionary pair values into dataframe

    profit_lost_list = []
    absolute_list = []
    financial_year = financial_year_dict(df_transactions)

    for subset_pair in dict_subset:
        try:
            print(f"Processing Pair: {subset_pair}")
            result = calculate_gains(dict_subset[subset_pair], financial_year)
            profit_lost_list.append(result[0])
            absolute_list.append(result[1])
            print('')
        except Exception as e:
            print(str(e) + f" for {subset_pair}")
            print('')
    print("________________________________________________\n")        
    print("p/l is the net profit\nabsolute is the total of only positive trades\n")
    
    print(f'total net p/l: {sum(profit_lost_list)}')
    print(f'total absolute gains: {sum(absolute_list)}\n')

    for year in financial_year:
        print(f'fy{year}-{year+1}, net profit of {financial_year[year].get("profit")}')
        print(f'fy{year}-{year+1}, absolute of {financial_year[year].get("absolute")}')


def financial_year_dict(df: pd.DataFrame):
    financial_year = {}
    for year in df['Date(UTC)'].dt.year.unique():
        financial_year[year-1] = {'profit': 0, 'absolute': 0}
    return financial_year


def cleanup(df: pd.DataFrame):
    df['Date(UTC)'] = df['Date(UTC)'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y %H:%M"))
    df = df.sort_values(by='Date(UTC)', ascending=True)
    df['Executed'] = df['Executed'].str.extract(r'(\d*\,*.*\d+)', expand=False).apply(
        lambda x: Decimal(x.replace(',', '')))
    df['Price'] = df['Price'].apply(lambda x: Decimal(x.replace(',', '')))
    df['Fee'] = df['Fee'].str.extract(r'(\d*\,*.*\d+)', expand=False).apply(
        lambda x: Decimal(x.replace(',', '')))
    return df


def convert_prices_to_aud(df: pd.DataFrame, conversion: pd.DataFrame):
    for index, row in df.iterrows():
        if str(row['Amount']).__contains__("USD"):
            try:
                df.Price[index] *= Decimal(str(conversion.loc[conversion['Date'] == row['Date(UTC)'].strftime("%d/%m/%Y")]['Price'].values[0]))
            except IndexError as e:
                date = row['Date(UTC)'].strftime("%d/%m/%Y")
                print(f'{date} No price rate history please check source')

        if str(row['Amount']).__contains__("USD"):
            try:
                df.Fee[index] *= Decimal(str(conversion.loc[conversion['Date'] == row['Date(UTC)'].strftime("%d/%m/%Y")]['Price'].values[0]))
            except IndexError as e:
                date = row['Date(UTC)'].strftime("%d/%m/%Y")
                print(f'{date} No price rate history please check source')


def calculate_gains(df_pair: pd.DataFrame, financial_year: dict):
    grouped_side_pair = group_sides(df_pair)
    dict_side = {'BUY': pd.DataFrame(grouped_side_pair.get_group('BUY')).reset_index(drop=True),
                 'SELL': pd.DataFrame(grouped_side_pair.get_group('SELL').reset_index(drop=True))}
    return calculate_pn(dict_side, financial_year)


def calculate_pn(dict_side: dict, financial_year: dict):
    absolute_gains = 0
    profit_loss = 0
    for buy_index, buy in dict_side['BUY'].iterrows():
        for sell_index, sell in dict_side['SELL'].iterrows():
            sell_quantity = 0
            sub_profit_loss = 0
            sell_fee = 0
            price_diff = dict_side['SELL'].Price[sell_index] - dict_side['BUY'].Price[buy_index]

            if (dict_side['BUY'].Executed[buy_index] == 0) or (dict_side['SELL'].Executed[sell_index] == 0):
                continue
            elif dict_side['BUY'].Executed[buy_index] > dict_side['SELL'].Executed[sell_index]:
                sell_quantity = dict_side['SELL'].Executed[sell_index]
                dict_side['BUY'].Executed[buy_index] -= dict_side['SELL'].Executed[sell_index]
                dict_side['SELL'].Executed[sell_index] = 0
                sell_fee = dict_side['SELL'].Fee[sell_index]
                dict_side['SELL'].Fee[sell_index] = 0
            else:
                sell_quantity = dict_side['BUY'].Executed[buy_index]
                dict_side['SELL'].Executed[sell_index] -= dict_side['BUY'].Executed[buy_index]
                dict_side['BUY'].Executed[buy_index] = 0

            sub_profit_loss = (sell_quantity * price_diff) - sell_fee
            absolute_gains += is_positive_gain(sub_profit_loss)

            # 1 year gap CGT discount
            if relativedelta(dict_side['SELL']['Date(UTC)'][sell_index],
                             dict_side['BUY']['Date(UTC)'][buy_index]).years > 0 and sub_profit_loss >= 0:
                profit_loss += sub_profit_loss / 2
            else:
                if dict_side['SELL']['Date(UTC)'][sell_index].month > 6:
                    financial_year[dict_side['SELL']['Date(UTC)'][sell_index].year]['profit'] += sub_profit_loss
                    financial_year[dict_side['SELL']['Date(UTC)'][sell_index].year]['absolute'] += is_positive_gain(sub_profit_loss)
                else:
                    financial_year[dict_side['SELL']['Date(UTC)'][sell_index].year-1]['profit'] += sub_profit_loss
                    financial_year[dict_side['SELL']['Date(UTC)'][sell_index].year-1]['absolute'] += is_positive_gain(sub_profit_loss)

                profit_loss += sub_profit_loss

    print(f"Net Profit/Loss: {profit_loss} \n Absolute Gains: {absolute_gains}")
    return profit_loss, absolute_gains


def is_positive_gain(profit: int):
    if profit > 0:
        return profit
    else:
        return 0


def group_sides(df_pair: pd.DataFrame):
    group_side = df_pair.groupby('Side')

    if group_side.size().size != 2:
        raise Exception("Pair doesnt have both BUY and SELL")

    return group_side


def format_into_df(subset_pairs):
    d = {}  # d = collections.defaultdict(dict)

    for subset_pair in subset_pairs:
        df_subset_pair = pd.DataFrame(subset_pair)
        d[df_subset_pair.iloc[0, 1]] = df_subset_pair.reset_index(drop=True)  # grabs column 'pair' as name from df

    return d


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
