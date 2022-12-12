import pandas as pd
from urllib.parse import urlparse, parse_qs
from process_helpers import Interaction


pd.options.mode.chained_assignment = None



def analyze_interaction(interaction_df):
    """First, let's extract the keywords if available. We'll look through any rows
    that have values of 0 or 2 within event_list. Look for the FIRST occurrence of a keyword
    based on hit_time_gmt
    """
    # Replace empty cells with 0
    interaction_df['event_list'].fillna(0, inplace=True)

    # Change event_list data type to int
    interaction_df['event_list'] = interaction_df['event_list'].astype(int)

    # Create interaction object in order to perform business logic
    interaction = Interaction(interaction_df)

    # Get keyword and search domain associated with single customer website interaction
    keyword_domain = interaction.get_keyword_search_domain()
    keyword = keyword_domain[0]
    search_domain = keyword_domain[1]

    # Get possible revenue that is associated with the keyword and domain
    revenue = interaction.get_revenue()

    # append all relevant metrics for single interaction
    interaction_metrics = [search_domain, keyword, revenue]

    return interaction_metrics






def process_interactions(file_location):
    """
    Break up dataframe into groups based on IP address.
    Each group will be called an "interaction".

    For each interaction, run business logic in order to find the search keyword, domain, and revenue.
    Finally, aggregate all interaction metrics into a single dataframe, grouped by keyword and domain

    :return: A single dataframe
    """

    hit_df = pd.read_csv(file_location, sep='\t', header=0)

    # sort entire df by time, so no need to sort on individual groups later
    hit_df.sort_values(by='hit_time_gmt', ascending=True)

    # Group by ip to represent individual session
    ip_groups = hit_df.groupby('ip')

    # Treat each IP address as a separate group for customer journey (event_list analysis)
    group_df_list = [ip_groups.get_group(x) for x in ip_groups.groups]

    interaction_metrics_list = []
    for g in group_df_list:
        interaction_metrics = analyze_interaction(g)
        interaction_metrics_list.append(interaction_metrics)

    total_metrics_df = pd.DataFrame(interaction_metrics_list, columns = ['Search Engine Domain', 'Search Keyword', 'Revenue'])

    # Group by domain and keyword and sum revenue
    total_metrics_df = total_metrics_df.groupby(['Search Engine Domain','Search Keyword'])['Revenue'].sum().reset_index()
    total_metrics_df = total_metrics_df.round(2)

    # sort by revenue descending
    total_metrics_df = total_metrics_df.sort_values(by='Revenue', ascending=False)
    total_metrics_df = total_metrics_df.reset_index(drop=True)

    print(total_metrics_df)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    file_location = 'data[57][88][30].tsv'
    process_interactions(file_location)
