from utils.file_utils import replace_special_chars_to_underscore

company = ("セールス：スリーエムヘルスケア_ジャパン_フィルター製品事業部\u3000販売部\u3000"
        "製薬・バイオ医薬品_Sales_of_liquid_filters_for_Biopharmaceuticals_in_the_--")
company = replace_special_chars_to_underscore(company, 100)
job = ("----------Sales-of-liquid-filters-for-Biopharmaceuticals-in-the-Separation-and-Purification-Sciences-Division"
       "-of-3M-Healthcare-Japan_R01121986")

print("company ", company)
print("job ", job)
