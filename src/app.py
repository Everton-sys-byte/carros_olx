# %%
import etl 
import analytics

# fazendo o etl e carregando os dados no postgres e também localmente na pasata data/carros
etl.startExtraction()
analytics.createAnalyticsTables()
# %%
