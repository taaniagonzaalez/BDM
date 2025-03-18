from API_GM import * 
import pandas as pd

review_creator_path = 'utilities/review_creator.json'
file_barcelona = 'dataframes/restaurants_bcn.csv'

with open(review_creator_path, "r", encoding="utf-8") as file:
    review_creator = json.load(file)

if __name__ == "__main__":

    df_bcn = pd.read_csv(file_barcelona)
    restaurant_names = df_bcn['name'].to_list()

    for restaurant in restaurant_names:

        restaurant_info = dict(df_bcn[df_bcn['name'] == restaurant])

        API = API_GM(restaurant, review_creator)

        ranking = API.generate_ranking()

        reviews = API.generate_multiple_reviews()

        status = API.get_current_status()

        conc = API.get_concurrency()









