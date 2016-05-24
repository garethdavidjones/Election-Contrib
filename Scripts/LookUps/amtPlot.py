import matplotlib.pylab as plt
import seaborn as sns
import pandas as pd


output = [('', 375), ('C', 740752), ('I', 4391578)]
df = pd.DataFrame(output, columns=["Type", "Count"])
df.head()

sns.barplot(df.Type, df.Count)
plt.title("Contributer Type")
plt.show()
