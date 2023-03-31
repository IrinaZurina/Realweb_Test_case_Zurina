/*В представленной базе данных есть две массивных таюлицы, которые
имеют общие данные, которые согласно реляционному подходу, необходимо
вынести в отдельные справочные таблицы и реализовать между ними и таблицами,
несущими основную информаци, связи типа "один-ко многим".

К сожалению, у меня не было опыта взаимодействия с BigQuery, и не получилось создать 
новые отдельные таблицы из существующих. Поэтому далее представлена моя логика,
которая могла быть быть осуществлена именно через создание таких таблиц.

Одной из основных сложностей оказалось определить, к какой команде относится
каждый питчер, так как в представленных таблицах они указаны в одной строке
с принимающей и гостевой командами.

По моей логике, для того, чтобы вычислить эти данные, надо посчитать, какая команда
упоминается для каждого питчера чаще всего. Для этого я объединила данные из таблиц 
games_post_wide и games_wide в единую таблицу, состоящую из двух столбцов - 
идентификатора питчера и упомянутых в связи с ним команд из столбцов awayTeamName и homeTeamName
*/

SELECT pitcherId, awayTeamName AS Team  FROM `bigquery-public-data.baseball.games_post_wide` 
WHERE pitcherId IS NOT NULL
UNION ALL
SELECT pitcherId, homeTeamName AS Team  FROM `bigquery-public-data.baseball.games_post_wide`
WHERE pitcherId IS NOT NULL
UNION ALL 
SELECT pitcherId, awayTeamName AS Team  FROM `bigquery-public-data.baseball.games_wide` 
WHERE pitcherId IS NOT NULL
UNION ALL
SELECT pitcherId, homeTeamName AS Team  FROM `bigquery-public-data.baseball.games_wide`
WHERE pitcherId IS NOT NULL
GROUP BY pitcherID, Team
ORDER BY pitcherId;

/* Полученную таюлицу сохранила как временную: `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anona51b129501fc39294731a0bc05c407f2f507c28b628138d449796b47ffbfb993`

Далее в полученной таблице произвела подсчет упоминания команд в связи с каждым питчером, отсортировала
их по убыванию и вывела только первую строку для каждого питчера - таким образом данный запрос формирвет связь
питчер-команда.
Сохраняем как временную таблицу ``
*/
SELECT pitcherId, Team, ROW_NUMBER() OVER (PARTITION BY pitcherId, Team ORDER BY counter DESC) rn
FROM (SELECT pitcherId, Team, COUNT(Team) as counter 
      FROM `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anona51b129501fc39294731a0bc05c407f2f507c28b628138d449796b47ffbfb993` 
      GROUP BY pitcherId, Team);

/* Находим максимальную скорость броска для каждого питчера из двух исходных таблиц
Сохраняем временную таблицу `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anon9abdc4824110fe33e9c807dbd984dc81a64f471a21c58b6fb650c0053d72f576`*/

SELECT pitcherId, MAX(pitchSpeed) AS max_speed FROM
(SELECT pitcherId, pitchSpeed  FROM `bigquery-public-data.baseball.games_post_wide` 
WHERE pitcherId IS NOT NULL
UNION ALL 
SELECT pitcherId, pitchSpeed  FROM `bigquery-public-data.baseball.games_wide` 
WHERE pitcherId IS NOT NULL)
GROUP BY pitcherId
;

/* Создаем запрос для поиска соответствия идентификатора питчера его имени и фамилии
Сохраняем временную таблицу `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anon4bebea12fec508c536ec9f9b9bb6a67c65dee7676af7320abc25c2153240b2ed`*/
SELECT DISTINCT(pitcherId), pitcherFirstName, pitcherLastName FROM
(SELECT pitcherId, pitcherFirstName, pitcherLastName FROM `bigquery-public-data.baseball.games_post_wide` 
WHERE pitcherId IS NOT NULL
UNION ALL
SELECT pitcherId, pitcherFirstName, pitcherLastName  FROM `bigquery-public-data.baseball.games_wide` 
WHERE pitcherId IS NOT NULL
GROUP BY pitcherID, pitcherFirstName, pitcherLastName
ORDER BY pitcherId);

/*Последним этапом объединяем три временные таблицы и оттуда после группировки по командам и сортировки
по максимальной скорости броска для каждого питчера находим максимальную скорость броска для каждой команды
и игрока, который произвел этот бросок.
Результаты отсортированы по названию команды */
SELECT Team, pitcherFirstName, pitcherLastName, max_speed FROM
(SELECT Team, pitcherFirstName, pitcherLastName, max_speed, ROW_NUMBER() OVER (PARTITION BY Team ORDER BY max_speed DESC) rn
FROM (SELECT Team, max_speed, pitcherFirstName, pitcherLastName
    FROM `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anon9abdc4824110fe33e9c807dbd984dc81a64f471a21c58b6fb650c0053d72f576`
    JOIN `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anon488b8aaa1a73bd539b98e265f131023c7b05b6db9bf3c15b555fa63910cd434c` USING(pitcherId)
    JOIN `bigquery-public-data-382212._bc11c4a3f61695fa4c835326c60273f06e7685ce.anon4bebea12fec508c536ec9f9b9bb6a67c65dee7676af7320abc25c2153240b2ed` USING(pitcherId)
    GROUP BY Team, max_speed, pitcherFirstName, pitcherLastName)
GROUP BY Team, pitcherFirstName, pitcherLastName, max_speed)
WHERE rn = 1
ORDER BY Team;