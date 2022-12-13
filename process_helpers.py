import pandas as pd
from urllib.parse import urlparse, parse_qs

class Interaction:
    def __init__(self, interaction_df):
        self.interaction_df = interaction_df

    def get_keyword_search_domain(self):
        # Filter df events to 0 and 2 and then sort by time.
        keyword_df = self.interaction_df.loc[(self.interaction_df['event_list'] == 0) | (self.interaction_df['event_list'] == 2)]

        # Create dictionary and parse for keywords. If found, get search website domain.
        keyword_dict = keyword_df.to_dict()

        keyword = None
        search_domain = None

        for key, value in keyword_dict["referrer"].items():

            # create url object in order to parse search terms
            url_object = urlparse(value)
            if url_object.query:
                url_query = (url_object.query)
                parsed_query = parse_qs(url_query)
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

                    # Get search url domain
                    search_domain = url_object.netloc

                    # Only need first valid keyword and search link  from referred list. Can break loop once found.
                    break
                else:
                    pass

        return keyword, search_domain

    def get_revenue(self):
        revenue_df = self.interaction_df.loc[(self.interaction_df['event_list'] == 1)]

        # if not empty, parse product_list for total revenue
        if not revenue_df.empty:
            product_list = revenue_df.iloc[0]['product_list']
            sep_product_list = product_list.split(';')
            revenue = float((sep_product_list[3]))
        else:
            revenue = 0.0

        return revenue




