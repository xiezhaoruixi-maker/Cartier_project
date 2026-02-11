import pandas as pd
import numpy as np

# ============================================================================
# STEP 1: 读取原始数据
# ============================================================================

df_2022 = pd.read_csv('baseline_2022_fe_csv.csv')
df_2026 = pd.read_csv('baseline_2026_fe_20260211_142006.csv')

print(f"2022年原始数据: {len(df_2022)} 行")
print(f"2026年原始数据: {len(df_2026)} 行")

# ============================================================================
# STEP 2: 筛选腕表产品（2022数据包含珠宝，需要过滤）
# ============================================================================

watch_collections = [
    'Tank', 'Santos', 'Pasha', 
    'Panthère de Cartier', 'Panthère', 'Panthere',
    'Ballon Bleu de Cartier', 'Ballon Bleu', 
    'Baignoire', 'Santos de Cartier', 'Clé de Cartier'
]

df_2022_watches = df_2022[df_2022['collection'].isin(watch_collections)].copy()
print(f"2022年腕表数据: {len(df_2022_watches)} 行")

# ============================================================================
# STEP 3: 清理reference code以便匹配
# ============================================================================

# 2022数据的reference_code格式: "CRWT100015"
# 2026数据的local_reference格式: "WT100015"
# 需要去掉"CR"前缀才能匹配

df_2022_watches['ref_clean'] = df_2022_watches['reference_code'].str.replace('CR', '').str.strip()
df_2026['ref_clean'] = df_2026['local_reference'].str.strip()

# ============================================================================
# STEP 4: 标准化collection名称
# ============================================================================

# 两个数据集中collection名称不统一，需要映射
collection_mapping = {
    'Panthere': 'Panthère',
    'Panthère de Cartier': 'Panthère',
    'La Panthère de Cartier': 'Panthère',
    'Ballon Bleu': 'Ballon Bleu',
    'Ballon Bleu de Cartier': 'Ballon Bleu',
    'Santos': 'Santos',
    'Santos de Cartier': 'Santos',
    'Tank': 'Tank',
    'Pasha': 'Pasha',
    'Baignoire': 'Baignoire'
}

df_2022_watches['collection_std'] = df_2022_watches['collection'].map(collection_mapping).fillna(df_2022_watches['collection'])
df_2026['collection_std'] = df_2026['collection'].map(collection_mapping).fillna(df_2026['collection'])

# ============================================================================
# STEP 5: 转换所有价格为EUR（2022数据有多种货币）
# ============================================================================

# 2022数据包含多种货币: EUR, USD, GBP, CHF, CNY, AED, JPY
# 2026数据已经是EUR
# 使用近似汇率转换（实际应用中应使用2022年的真实汇率）

exchange_rates = {
    'EUR': 1.0,
    'USD': 0.92,    # 1 USD ≈ 0.92 EUR
    'GBP': 1.17,    # 1 GBP ≈ 1.17 EUR
    'CHF': 1.05,    # 1 CHF ≈ 1.05 EUR
    'CNY': 0.13,    # 1 CNY ≈ 0.13 EUR
    'AED': 0.25,    # 1 AED ≈ 0.25 EUR
    'JPY': 0.0065   # 1 JPY ≈ 0.0065 EUR
}

df_2022_watches['price_eur'] = df_2022_watches.apply(
    lambda row: row['price'] * exchange_rates.get(row['currency'], 1.0), 
    axis=1
)

# ============================================================================
# STEP 6: 匹配2022和2026的产品
# ============================================================================

# 只保留两个年份都有的产品
matched_2022 = df_2022_watches[df_2022_watches['ref_clean'].isin(df_2026['ref_clean'])].copy()
matched_2026 = df_2026[df_2026['ref_clean'].isin(matched_2022['ref_clean'])].copy()

print(f"可匹配产品数: {len(matched_2022)} 款")

# ============================================================================
# STEP 7: 合并数据
# ============================================================================

# 通过ref_clean合并，后缀标识来源
merged = matched_2022.merge(
    matched_2026[['ref_clean', 'price_eur', 'collection_std', 'title']], 
    on='ref_clean', 
    suffixes=('_2022', '_2026')
)

# ============================================================================
# STEP 8: 计算价格变化
# ============================================================================

merged['price_change_eur'] = merged['price_eur_2026'] - merged['price_eur_2022']
merged['price_change_pct'] = (
    (merged['price_eur_2026'] - merged['price_eur_2022']) / merged['price_eur_2022'] * 100
).round(2)

# ============================================================================
# STEP 9: 添加价格区间分类
# ============================================================================

def price_segment(price):
    if price < 5000:
        return 'Entry Level'
    elif price < 15000:
        return 'Accessible Luxury'
    elif price < 30000:
        return 'High Luxury'
    else:
        return 'Haute Horlogerie'

merged['price_segment'] = merged['price_eur_2026'].apply(price_segment)

# ============================================================================
# 生成CSV 1: cartier_tableau_data.csv (主数据文件 - 宽格式)
# ============================================================================

tableau_data = merged[[
    'ref_clean', 
    'collection_std_2022', 
    'material_label', 
    'size_label',
    'price_eur_2022', 
    'price_eur_2026', 
    'price_change_eur', 
    'price_change_pct',
    'price_segment', 
    'currency', 
    'title'
]].copy()

# 重命名列为更友好的名称
tableau_data.columns = [
    'Reference', 
    'Collection', 
    'Material', 
    'Size',
    'Price_2022_EUR', 
    'Price_2026_EUR', 
    'Price_Change_EUR', 
    'Price_Change_Pct',
    'Price_Segment', 
    'Original_Currency', 
    'Product_Name'
]

# 按涨幅降序排序
tableau_data = tableau_data.sort_values('Price_Change_Pct', ascending=False)

# 保存
tableau_data.to_csv('cartier_tableau_data.csv', index=False)
print(f"\n✓ cartier_tableau_data.csv 已生成 ({len(tableau_data)} 行)")

# ============================================================================
# 生成CSV 2: cartier_tableau_timeseries.csv (时间序列 - 长格式)
# ============================================================================

# 将宽格式转换为长格式，便于Tableau创建时间序列图
long_format = []

for _, row in tableau_data.iterrows():
    # 2022年数据点
    long_format.append({
        'Reference': row['Reference'],
        'Collection': row['Collection'],
        'Material': row['Material'],
        'Size': row['Size'],
        'Price_Segment': row['Price_Segment'],
        'Original_Currency': row['Original_Currency'],
        'Product_Name': row['Product_Name'],
        'Year': 2022,
        'Price_EUR': row['Price_2022_EUR']
    })
    # 2026年数据点
    long_format.append({
        'Reference': row['Reference'],
        'Collection': row['Collection'],
        'Material': row['Material'],
        'Size': row['Size'],
        'Price_Segment': row['Price_Segment'],
        'Original_Currency': row['Original_Currency'],
        'Product_Name': row['Product_Name'],
        'Year': 2026,
        'Price_EUR': row['Price_2026_EUR']
    })

long_df = pd.DataFrame(long_format)
long_df.to_csv('cartier_tableau_timeseries.csv', index=False)
print(f"✓ cartier_tableau_timeseries.csv 已生成 ({len(long_df)} 行)")

# ============================================================================
# 生成CSV 3: cartier_summary_stats.csv (总体统计)
# ============================================================================

summary_stats = pd.DataFrame({
    'Metric': [
        'Total Products', 
        'Average Increase %', 
        'Median Increase %', 
        'Max Increase %', 
        'Min Increase %'
    ],
    'Value': [
        len(tableau_data),
        tableau_data['Price_Change_Pct'].mean(),
        tableau_data['Price_Change_Pct'].median(),
        tableau_data['Price_Change_Pct'].max(),
        tableau_data['Price_Change_Pct'].min()
    ]
})

summary_stats.to_csv('cartier_summary_stats.csv', index=False)
print(f"✓ cartier_summary_stats.csv 已生成 ({len(summary_stats)} 行)")

# ============================================================================
# 生成CSV 4: cartier_collection_stats.csv (系列统计汇总)
# ============================================================================

# 按系列聚合统计
collection_stats = tableau_data.groupby('Collection').agg({
    'Price_Change_Pct': ['mean', 'median', 'count'],
    'Price_2022_EUR': 'mean',
    'Price_2026_EUR': 'mean'
}).round(2)

# 重命名多级列
collection_stats.columns = [
    'Avg_Increase_Pct', 
    'Median_Increase_Pct', 
    'Product_Count', 
    'Avg_Price_2022', 
    'Avg_Price_2026'
]

# 重置索引（Collection从行索引变为普通列）
collection_stats = collection_stats.reset_index()

collection_stats.to_csv('cartier_collection_stats.csv', index=False)
print(f"✓ cartier_collection_stats.csv 已生成 ({len(collection_stats)} 行)")

# ============================================================================
# 数据处理总结
# ============================================================================

print("\n" + "="*60)
print("数据处理完成总结")
print("="*60)
print(f"原始2022数据:        {len(df_2022):,} 行")
print(f"过滤后腕表数据:      {len(df_2022_watches):,} 行")
print(f"可匹配产品:          {len(merged):,} 款")
print(f"系列数量:            {tableau_data['Collection'].nunique()} 个")
print(f"平均涨幅:            {tableau_data['Price_Change_Pct'].mean():.2f}%")
print(f"中位数涨幅:          {tableau_data['Price_Change_Pct'].median():.2f}%")
print("="*60)

# ============================================================================
# 数据验证
# ============================================================================

print("\n数据验证:")
print(f"- 无空值检查: {tableau_data.isnull().sum().sum() == 0}")
print(f"- 价格为正数: {(tableau_data['Price_2022_EUR'] > 0).all()}")
print(f"- 系列分布:\n{tableau_data['Collection'].value_counts()}")
