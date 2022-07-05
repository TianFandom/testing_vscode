with latest as
(select {{ Channel_Filter }}, sum(revenue) Revenue_Latest, sum(margin) Margin_Latest, sum(sessions) Sessions_Latest, sum(orders) Orders_Latest, sum(new_customers) New_Customers_Latest, Orders_latest/Sessions_Latest*100 "% CVR_latest"
from web_analytics.snowplow_hybrid
where case '{{ Period_Filter }}'  when 'this_week_vs_last' then date between (select current_date - DATE_PART(dayofweek, current_date)::int + 1) and getdate() 
                                  when 'this_month_vs_last' then date between (select date_trunc('month', current_date)) and getdate()
                                  when 'this_quarter_vs_last' then date between (select date_trunc('quarter', current_date) ) and getdate()
                                  when 'last_week_vs_previous' then date between (select current_date - DATE_PART(dayofweek, current_date)::int - 6) and (select current_date - DATE_PART(dayofweek, current_date)::int + 1)
                                  when 'last_month_vs_previous' then date between (select date_add('month', -1, date_trunc('month', current_date))) and (select date_trunc('month', current_date)) 
                                  when 'last_quarter_vs_previous' then date between (select date_trunc('quarter', current_date) - interval '3 month') and (select date_trunc('quarter', current_date))
                                  when 'this_week_vs_last_year' then date between (select current_date - DATE_PART(dayofweek, current_date)::int + 1) and getdate() 
                                  when 'this_month_vs_last_year'then date between (select date_trunc('month', current_date)) and getdate()
                                  when 'this_quarter_vs_last_year' then date between (select date_trunc('quarter', current_date) ) and getdate()
                                  when 'last_week_vs_last_year' then date between (select current_date - DATE_PART(dayofweek, current_date)::int - 6) and (select current_date - DATE_PART(dayofweek, current_date)::int + 1)
                                  when 'last_month_vs_last_year' then date between (select date_add('month', -1, date_trunc('month', current_date))) and (select date_trunc('month', current_date)) 
                                  when 'last_quarter_vs_last_year' then date between (select date_trunc('quarter', current_date) - interval '3 month') and (select date_trunc('quarter', current_date))
                                  end
      and (geo_country = '{{ Country Code }}' or '{{ Country Code }}' = 'ALL')
      and ('ALL' in ({{ Channel }}) or channel in ({{ Channel }}))
      and ('ALL' in ({{ Sub Channel }}) or sub_channel in ({{ Sub Channel }}))
      and ('ALL' in ({{ landing_page_category }}) or landing_page_category in ({{ landing_page_category }}))
      and ('ALL' in ({{ Campaign }}) or campaign in ({{ Campaign }}))
      and ('ALL' in ({{ Source }}) or source in ({{ Source }}))
      and ('ALL' in ({{ Affiliate_website }}) or affiliate_website in ({{ Affiliate_website }}))
      and ('ALL' in ({{ Source }}) or source in ({{ Source }}))

group by 1
order by 2 desc),

    previous as 
(select {{ Channel_Filter }}, sum(revenue) Revenue_Previous, sum(margin) Margin_Previous, sum(sessions) Sessions_Previous, sum(orders) Orders_Previous, sum(new_customers) New_Customers_Previous, Orders_Previous/Sessions_Previous*100 "% CVR_Previous"
from web_analytics.snowplow_hybrid
where case '{{ Period_Filter }}'  when 'this_week_vs_last' then date between (select current_date - DATE_PART(dayofweek, current_date)::int -6) and getdate() - 7
                                  when 'this_month_vs_last' then date between (select date_trunc('month', current_date)- interval '1 month') and getdate()- interval '1 month'
                                  when 'this_quarter_vs_last' then date between (select date_trunc('quarter', current_date) - interval '3 month') and getdate() - interval '3 month'
                                  when 'last_week_vs_previous' then date between (select current_date - DATE_PART(dayofweek, current_date)::int - 13) and (select current_date - DATE_PART(dayofweek, current_date)::int - 7 + 1)
                                  when 'last_month_vs_previous' then date between (select date_add('month', -2, date_trunc('month', current_date))) and (select date_add('month', -1, date_trunc('month', current_date))) 
                                  when 'last_quarter_vs_previous' then date between (select date_trunc('quarter', current_date) - interval '6 month') and (select date_trunc('quarter', current_date)- interval '3 month')
                                  when 'this_week_vs_last_year' then date between (select current_date - DATE_PART(dayofweek, current_date)::int + 1) -365 and getdate() -365 
                                  when 'this_month_vs_last_year'then date between (select date_trunc('month', current_date)) - 365 and getdate() - 365
                                  when 'this_quarter_vs_last_year' then date between (select date_trunc('quarter', current_date) ) - 365 and getdate() - 365
                                  when 'last_week_vs_last_year' then date between (select current_date - DATE_PART(dayofweek, current_date)::int - 6) -365 and (select current_date - DATE_PART(dayofweek, current_date)::int + 1) -365
                                  when 'last_month_vs_last_year' then date between (select date_add('month', -1, date_trunc('month', current_date))) -365 and (select date_trunc('month', current_date)) -365
                                  when 'last_quarter_vs_last_year' then date between (select date_trunc('quarter', current_date) - interval '3 month') - 365 and (select date_trunc('quarter', current_date)) -365
                                  end
      and (geo_country = '{{ Country Code }}' or '{{ Country Code }}' = 'ALL')
                               
      and ('ALL' in ({{ Channel }}) or channel in ({{ Channel }}))
      and ('ALL' in ({{ Sub Channel }}) or sub_channel in ({{ Sub Channel }}))
      and ('ALL' in ({{ landing_page_category }}) or landing_page_category in ({{ landing_page_category }}))
      and ('ALL' in ({{ Campaign }}) or campaign in ({{ Campaign }}))
      and ('ALL' in ({{ Source }}) or source in ({{ Source }}))
      and ('ALL' in ({{ Affiliate_website }}) or affiliate_website in ({{ Affiliate_website }}))

group by 1
order by 2 desc),

    Marketing as
(select l.{{ Channel_Filter }}::varchar(255) as dimension, 
       Sessions_Previous, Sessions_Latest, (Sessions_Latest - Sessions_Previous)/Sessions_Previous *100 as "% Sessions Change",
       "% CVR_Previous", "% CVR_latest", ("% CVR_latest" - "% CVR_Previous")/"% CVR_Previous"*100 as "% CVR Change",
       Orders_Previous, Orders_Latest, (Orders_Latest - Orders_Previous)/Orders_Previous *100 as "% Orders Change",
       New_Customers_Previous, New_Customers_Latest, (New_Customers_Latest - New_Customers_Previous)/New_Customers_Previous *100 as "% New_Customers Change",
       Revenue_Previous Rev_Previous, nvl(Revenue_Latest, 0.0) Rev_Latest, (Rev_Latest - Rev_Previous)/nullif(Rev_Previous, 0.0) *100 as "% Revenue Change",
       Margin_Previous, Margin_Latest, (Margin_Latest - Margin_Previous)/Margin_Previous *100 as "% Margin Change",
       row_number() over (order by Rev_Latest desc) as Rev_Latest_Rank
from latest as l left join previous as p on l.{{ Channel_Filter }}=p.{{ Channel_Filter }}
where dimension is not null
)

select * from Marketing
--where Rev_Latest_Rank <= 12
--order by Rev_Latest desc
union 
select 'TOTAL'::varchar(255) as dimension,
       sum(Sessions_Previous), sum(Sessions_Latest), (sum(Sessions_Latest) - sum(Sessions_Previous))/sum(Sessions_Previous) *100,
     --avg("% CVR_Previous"), avg("% CVR_latest"), (avg("% CVR_latest") - avg("% CVR_Previous"))/avg("% CVR_Previous")*100 as "% CVR Change",
       sum(Orders_Previous)/sum(Sessions_Previous) *100, sum(Orders_Latest)/sum(Sessions_Latest) *100 , (sum(Orders_Latest)/sum(Sessions_Latest) - sum(Orders_Previous)/sum(Sessions_Previous))/(sum(Orders_Previous)/sum(Sessions_Previous)) * 100,
       sum(Orders_Previous), sum(Orders_Latest), (sum(Orders_Latest) - sum(Orders_Previous))/sum(Orders_Previous) *100,
       sum(New_Customers_Previous), sum(New_Customers_Latest), (sum(New_Customers_Latest) - sum(New_Customers_Previous))/sum(New_Customers_Previous) *100,
       sum(Rev_Previous), sum(Rev_Latest), (sum(Rev_Latest) - sum(Rev_Previous))/nullif(sum(Rev_Previous), 0.0) *100,
       sum(Margin_Previous), sum(Margin_Latest), (sum(Margin_Latest) - sum(Margin_Previous))/sum(Margin_Previous) *100,
       count(*)+1 as Rev_Latest_Rank
from Marketing
group by 1
order by Rev_Latest desc
