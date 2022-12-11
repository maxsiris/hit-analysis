import csv
import pandas as pd
from urllib.parse import urlparse, parse_qs

hit_df = pd.read_csv('data[57][88][30].tsv', sep='\t', header=0)

# sort entire df by time, so no need to sort on individual groups later
hit_df.sort_values(by='hit_time_gmt', ascending=True)

# Group by ip to represent individual session
ip_groups = hit_df.groupby('ip')



def analyze_interaction(interaction_df):
    """First, let's extract the keywords if available. We'll look through any rows
    that have values of 0 or 2 within event_list. Look for the FIRST occurrence of a keyword
    based on hit_time_gmt
    """
    # Replace empty cells with 0
    interaction_df['event_list'].fillna(0, inplace=True)

    # Change datatype to string
    interaction_df['event_list'] = interaction_df['event_list'].astype(int)

    # Filter df events to 0 and 2 and then sort by time.
    keyword_df = interaction_df.loc[(interaction_df['event_list'] == 0) | (interaction_df['event_list'] == 2)]

    # Create dictionary and parse for keywords. If found, get search website domain.
    keyword_dict = keyword_df.to_dict()

    # print(keyword_dict)

    # Create list of known conditions that indicates a keyword is present
    keyword_indicator_list = []

    for key, value in keyword_dict["referrer"].items():
        url_object = urlparse(value)
        if url_object.query:
            url_query = (url_object.query)
            # print(url_query)
            parsed_query = parse_qs(url_query)
            # print(parsed_query)
            # print(type(parsed_query))
            key_list = [k for k, v in parsed_query.items()]
            if 'p' in key_list:
                parse_key = 'p'
            elif 'q' in key_list:
                parse_key = 'q'
            else:
                parse_key = None

            if parse_key:
                keyword = parsed_query[parse_key][0]

                # Normalize keyword
                keyword = keyword.upper()
                print(keyword)
            else:
                pass






# Treat each IP address as a separate group for customer journey (event_list analysis)
group_df_list = [ip_groups.get_group(x) for x in ip_groups.groups]

# group_df_list = [group_df_list[1]]
for g in group_df_list:
    analyze_interaction(g)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    pass
