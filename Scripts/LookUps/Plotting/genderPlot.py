import matplotlib.pylab as plt
import seaborn as sns
import pandas as pd


output = [('Massive', 5023), ('Very Large', 5976), ('Collosal', 384112), ('Small', 2486770), ('Tiny', 5935745), ('Medium', 1390025), ('Large', 59267)]

df = pd.DataFrame(output, columns=["Type", "Count"])
df.head()

sns.barplot(df.Type, df.Count)
plt.title("Donation Sizes")
plt.show()
