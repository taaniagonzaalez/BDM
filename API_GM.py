import numpy as np
import random
from datetime import datetime
import json

"""

GET FAKE REVIEWS FROM GOOGLE MAPS

"""

class API_GM():
    def __init__(self, restaurant, info_file):
        self.restaurant = restaurant
        self.info_file = info_file

    def generate_ranking(self):
        # Generate a random number from 0 to 5
        ranking = round(np.clip(np.random.normal(loc=4, scale=1), 0, 5),1)
        return ranking
    
    def generate_review(self,ranking):
        # Generate a review
        if ranking < 3:
                opinion = 'negative'
        else:
                opinion = 'positive'
        full_review = " ".join([
                                random.choice(self.info_file["opening"]).format(self.restaurant),
                                random.choice(self.info_file[f"food_{opinion}"]),
                                random.choice(self.info_file[f"service_{opinion}"])
                            ])
        return {'name' : random.choice(self.info_file["client_name"]),
                'ranking': round(float(np.clip(np.random.normal(loc=ranking,scale=0.5), ranking-0.2,ranking+0.2)),1),
                'review' : full_review
                }
    
    def generate_multiple_reviews(self, num=10):
        # Generate multiple reviews
        reviews = {'reviews' : []}
        for _ in range(num):
              ranking = self.generate_ranking()
              review = self.generate_review(ranking)
              reviews['reviews'].append(review)
        return reviews
    
    def get_current_status(self):
        # Get a closed or opened output
        rn = datetime.now().hour
        if 11 <= rn <= 17 or 20 <= rn <= 00:
             status = 'Opened'
        else:
             status = 'Closed'
        return status
    
    def get_concurrency(self):
        # Look if the restaurant is crowded or not (rush hours)
        rn = datetime.now().hour
        if 13 <= rn <= 15 or 21 <= rn <= 22:
             conc = 'The restaurant is more crowded than usual.'
        else:
             conc = None
        return conc
