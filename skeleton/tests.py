from .assignment_12 import *

sf=Scan(filepath="../data/friends.txt",outputs=None)
sr=Scan(filepath="../data/movie_ratings.txt",outputs=None)
se1=Select(inputs=[sf],predicate={"UID1":'1190'},outputs=None)
se2=Select(inputs=[sr],predicate={"MID":'16015'},outputs=None)
join=Join(left_inputs=[se1],right_inputs=[se2],outputs=None,left_join_attribute="UID2",right_join_attribute="UID")
proj=Project(inputs=[join],outputs=None,fields_to_keep=["Rating"])
groupby=GroupBy(inputs=[proj],outputs=None,key="",value="Rating",agg_gun="AVG")
groupby.get_next()