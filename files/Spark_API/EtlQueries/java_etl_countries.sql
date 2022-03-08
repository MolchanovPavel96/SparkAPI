select ms.name,
       ms.code,
       cw.region,
       cw.population,
       cw.area_sq_miles,
       ms.year_2014 as spending_2014_usd,
       ms.year_2014 * dc.course as spending_2014_rub,
       le.life_expectancy
from datasets.military_spending ms
cross join datasets.dollar_courses dc
  on dc.dt = '31-12-2014'
left join datasets.life_expectancy le
  on le.year = '2014' and case when le.country = 'Russia' and ms.name = 'Russian Federation'
                               then 1=1
                               else le.country = ms.name
                               end
left join datasets.countries_of_the_world cw
  on case when cw.country = 'Russia' and ms.name = 'Russian Federation'
          then 1=1
          else cw.country = ms.name
          end
where ms.type = 'Country'
and ms.indicator_name = 'Military expenditure (current USD)'
order by ms.year_2014 desc