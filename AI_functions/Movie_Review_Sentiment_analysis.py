# New notebook

# In[9]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.

%pip install -q --force-reinstall openai==1.99.5 2>/dev/null


# In[10]:


import synapse.ml.aifunc as aifunc
import pandas as pd

# Optional import for progress bars. In future versions, this import will be included by default
# Controlled by aifunc.default_conf.use_progress_bar and conf parameter of AI functions
from tqdm.auto import tqdm
tqdm.pandas()


# In[11]:


import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/movie_reviews_dataset.csv"
df = pd.read_csv("/lakehouse/default/Files/movie_reviews_dataset.csv")
display(df)


# In[13]:


df["sentiment"] = df["ReviewComment"].ai.analyze_sentiment()
display(df)


# In[15]:


new_df = spark.createDataFrame(df)
display(new_df)


# In[16]:


# Save DataFrame as a managed Delta table in the Lakehouse
new_df.write.format("delta").mode("overwrite").saveAsTable("Movie_with_sentiment")


# In[17]:


spark.sql("SHOW TABLES").show()


# In[ ]:





