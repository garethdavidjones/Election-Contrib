import matplotlib.pylab as plt
import seaborn as sns
import pandas as pd


output = [('Collosal', 384112), ('Massive', 5023), ('Very Large', 5976), ('Large', 59267), ('Medium', 1390025), ('Small', 2486770), ('Tiny', 5935745)]
df = pd.DataFrame(output, columns=["Type", "Count"])
df.head()

sns.barplot(df.Type, df.Count)
plt.title("Distribution of Amounts")
plt.show()
