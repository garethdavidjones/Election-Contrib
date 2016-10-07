import matplotlib.pylab as plt
import seaborn as sns
import pandas as pd


output = [('', 1605420), ('COMM', 1454816), ('CAND', 2072469)]
#('', 2181964), 


df = pd.DataFrame(output, columns=["Type", "Count"])
df.head()

sns.barplot(df.Type, df.Count)
plt.title("Recipient Type")
plt.show()
