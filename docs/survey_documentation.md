# Survey Documentation

## Overview

Total questions: 22

Total variables: 75

## Questions

### Q1: Welche drei Worte verbindest du spontan mit [U25]? 

**Type:** input_multiple_singleline

**Variables:** 3

| Variable   | Label             | Data Type |
|------------|-------------------|-----------|
| Q1_erstes  | Erstes Textfeld:  | String    |
| Q1_zweites | Zweites Textfeld: | String    |
| Q1_drittes | Drittes Textfeld: | String    |

### Q2: Wie lange bist du schon bei [U25]?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                                                       |
|----------|-----------|-----------------------------------------------------------------------|
| Q2       | String    | weniger als 1 Jahr; 1-2 Jahre; 3-4 Jahre; 5-6 Jahre; mehr als 6 Jahre |

### Q3: Wie bist du zu [U25] gekommen?

**Type:** multiple_choice

**Variables:** 8

| Variable       | Label                                                                                                                             | Data Type |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|
| Q3_erzählung   | Eine andere Person hat mich mitgenommen, bzw. mir von diesem Ehrenamt erzählt.                                                    | Boolean   |
| Q3_engagement  | Ich habe mich zuvor schon woanders ehrenamtlich engagiert und habe dort davon gehört.                                             | Boolean   |
| Q3_schule      | Ich habe in Uni, Fachhochschule oder Schule davon erfahren.                                                                       | Boolean   |
| Q3_einrichtung | Ich habe [U25] in einer anderen Organisation/ Einrichtung kennengelernt (z.B. Kirchengemeinde, Jugendgruppe, Beratungsstelle, …). | Boolean   |
| Q3_internet    | Ich habe über das Internet (z. B. Social Media, YouTube) von [U25] erfahren.                                                      | Boolean   |
| Q3_presse      | Ich habe über Presseberichte (Tageszeitung, TV, Radio) von [U25] erfahren.                                                        | Boolean   |
| Q3_ratsuchende | Ich war selbst Ratsuchende*r.                                                                                                     | Boolean   |
| Q3_sonstiges   | Sonstiges:                                                                                                                        | Boolean   |

### Q4: Nachfolgend findest du einige Aussagen zu [U25]. Bitte bewerte diese auf der Skala:

**Type:** matrix

**Variables:** 14

| Variable         | Label                                                                                                              | Data Type | Range  |
|------------------|--------------------------------------------------------------------------------------------------------------------|-----------|--------|
| Q4_bedenken      | Ich habe Bedenken, ob ich den Anforderungen gerecht werden kann.                                                   | Int64     | [1, 6] |
| Q4_sicherheit    | Ich merke, dass ich im Laufe der Zeit immer sicherer im Umgang mit Mails bei [U25] geworden bin.                   | Int64     | [1, 6] |
| Q4_lernen        | Bei [U25] kann ich mich ausprobieren und dazulernen.                                                               | Int64     | [1, 6] |
| Q4_entwicklung   | Ich habe das Gefühl, dass ich mich persönlich weiterentwickelt habe, seitdem ich bei [U25] bin.                    | Int64     | [1, 6] |
| Q4_entscheidung  | Es war eine gute Entscheidung bei [U25] mitzumachen.                                                               | Int64     | [1, 6] |
| Q4_hilfe         | Ich kann bei [U25] anderen weiterhelfen.                                                                           | Int64     | [1, 6] |
| Q4_motivation    | Die Peerteams/ Supervision steigern meine Motivation für [U25].                                                    | Int64     | [1, 6] |
| Q4_anwendung     | Dinge, die ich bei [U25] gelernt habe, kann ich auch woanders einsetzen (z. B. in Schule, in Arbeit).              | Int64     | [1, 6] |
| Q4_psychohygiene | Es gelingt mir, gut für mich zu sorgen und mich gut um meine Psychohygiene zu kümmern.                             | Int64     | [1, 6] |
| Q4_kompetenz     | Durch mein Engagement bei [U25] habe ich meine Kompetenz im Umgang mit psychischen Krisen deutlich erweitert.      | Int64     | [1, 6] |
| Q4_umgang        | Ich gehe aufgrund der Erfahrungen bei [U25] auch sicherer mit psychischen Krisen bei Menschen in meinem Umfeld um. | Int64     | [1, 6] |
| Q4_unterstützung | Falls ich einmal nicht weiterkomme oder mir ein Fehler unterläuft, weiß ich, an wen ich mich wenden kann.          | Int64     | [1, 6] |
| Q4_vorbereitung  | Die Peer-Ausbildung [U25] hat mich gut vorbereitet.                                                                | Int64     | [1, 6] |
| Q4_themen        | Bei [U25] setzt ich mich auch mit persönlichen Themen auseinander.                                                 | Int64     | [1, 6] |

### Q5: Wie geht es dir damit aktuell? Ist der Aufwand…

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                                           |
|----------|-----------|-----------------------------------------------------------|
| Q5       | String    | Viel zu wenig; Eher zu viel; Genau richtig; Eher zu wenig |

### Q6: Hast du neben [U25] noch andere Ehrenämter?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                                                                                           |
|----------|-----------|-----------------------------------------------------------------------------------------------------------|
| Q6       | String    | Ja, im gleichen Bereich; Ja, in einem anderen Bereich; Nein und das ist gut so; Nein, aber ich würde gern |

### Q8: Warum engagierst du dich bei [U25] ?

**Type:** multiple_choice_other

**Variables:** 15

| Variable              | Label                                                                               | Data Type |
|-----------------------|-------------------------------------------------------------------------------------|-----------|
| Q8_helfen             | Ich möchte anderen helfen.                                                          | Boolean   |
| Q8_weitergeben        | Ich möchte anderen weitergeben, was ich selbst in psychischen Krisen gelernt habe.  | Boolean   |
| Q8_wichtig            | Ich finde das Thema Suizidprävention gesellschaftlich wichtig.                      | Boolean   |
| Q8_sinnvoll           | Ich möchte mit meinem Engagement etwas Sinnvolles tun.                              | Boolean   |
| Q8_entwicklung        | Ich möchte mich persönlich weiterentwickeln.                                        | Boolean   |
| Q8_lernen             | Ich möchte im Bereich psychische Gesundheit dazulernen.                             | Boolean   |
| Q8_berufsorientierung | Ich sehe das Engagement als Möglichkeit zur Berufsorientierung.                     | Boolean   |
| Q8_qualifikation      | Ich sehe mein Engagement bei [U25] als relevante Zusatzqualifikation im Lebenslauf. | Boolean   |
| Q8_kontakte           | Ich schätze bei [U25] soziale Kontakte zu Peers und Koordinator*innen.              | Boolean   |
| Q8_verpflichtet       | Ich fühle mich zur Ausübung eines ehrenamtlichen Engagements verpflichtet.          | Boolean   |
| Q8_werte              | [U25] entspricht meinen persönlichen Werten.                                        | Boolean   |
| Q8_notwendig          | Ich engagiere mich, weil [U25] ohne Freiwillige nicht funktionieren würde.          | Boolean   |
| Q8_ausprobieren       | Ich sehe es als Möglichkeit mich auszuprobieren.                                    | Boolean   |
| Q8_other              | Anderes                                                                             | Boolean   |
| Q8_other_text         | Anderes (Text)                                                                      | String    |

### Q9: Was hilft dir bei deinem Engagement?

**Type:** multiple_choice_other

**Variables:** 11

| Variable         | Label                                                                                | Data Type |
|------------------|--------------------------------------------------------------------------------------|-----------|
| Q9_rückmeldungen | positive Rückmeldungen von Ratsuchenden erhalten                                     | Boolean   |
| Q9_freude        | Freude bei Ratsuchenden erleben                                                      | Boolean   |
| Q9_wirkung       | Wirkung der eigenen Arbeit sehen                                                     | Boolean   |
| Q9_sicherheit    | Sicherheitsgefühl bei der Beantwortung von Mails                                     | Boolean   |
| Q9_reflexion     | Peerteams/Supervisionen als strukturierte Reflexion                                  | Boolean   |
| Q9_belastung     | Unterstützung bei emotionaler Belastung                                              | Boolean   |
| Q9_begegnungen   | Begegnungen mit anderen Engagierten bei [U25]                                        | Boolean   |
| Q9_unterstützung | Unterstützung und Wertschätzung durch Hauptamtliche                                  | Boolean   |
| Q9_flexibilität  | Die Möglichkeit, Zeit selbstbestimmt einzubringen und auch mal eine Pause zu machen. | Boolean   |
| Q9_other         | Anderes                                                                              | Boolean   |
| Q9_other_text    | Anderes (Text)                                                                       | String    |

### Q10: Wie verbunden fühlst du dich mit anderen Peers vor Ort?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                                                                    |
|----------|-----------|------------------------------------------------------------------------------------|
| Q10      | String    | sehr verbunden; eher verbunden; neutral; eher nicht verbunden; gar nicht verbunden |

### Q11: Wie oft sprichst du mit Menschen aus deinem Umfeld über Themen wie psychische Gesundheit und Krisenintervention?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                      |
|----------|-----------|--------------------------------------|
| Q11      | String    | immer; häufig; manchmal; selten; nie |

### Q15: Zu welchem Standort gehörst du?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                                                                                              |
|----------|-----------|--------------------------------------------------------------------------------------------------------------|
| Q15      | String    | Berlin; Biberach; Dortmund; Dresden; Emsland; Freiburg; Gelsenkirchen; Hamburg; Münster; Nürnberg; Paderborn |

### Q12: Wie wahrscheinlich ist es, dass du deinen Freund*innen oder Bekannten empfiehlst, sich auch bei [U25] zu engagieren? 

**Type:** scale

**Variables:** 1

| Variable | Data Type | Range   |
|----------|-----------|---------|
| Q12      | Int64     | [0, 10] |

### Q13: Was würde dich davon abhalten, ein Engagement bei [U25] zu empfehlen?

**Type:** input_single_singleline

**Variables:** 1

| Variable | Data Type |
|----------|-----------|
| Q13      | String    |

### Q14: Was würdest du dir noch von [U25] wünschen?

**Type:** input_single_singleline

**Variables:** 1

| Variable | Data Type |
|----------|-----------|
| Q14      | String    |

### Q16: Dein Geschlecht:

**Type:** multiple_choice_other

**Variables:** 7

| Variable       | Label           | Data Type |
|----------------|-----------------|-----------|
| Q16_geschlecht | nicht-binär     | Boolean   |
| Q16_weiblich   | weiblich        | Boolean   |
| Q16_männlich   | männlich        | Boolean   |
| Q16_divers     | kein Geschlecht | Boolean   |
| Q16_angabe     | keine Angabe    | Boolean   |
| Q16_other      | Other           | Boolean   |
| Q16_other_text | Other (Text)    | String    |

### Q17: Dein Alter:

**Type:** input_single_integer

**Variables:** 1

| Variable | Label | Data Type |
|----------|-------|-----------|
| Q17      |       | Int64     |

### Q18: Was ist deine Haupttätigkeit?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values                                                                                                                           |
|----------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| Q18      | String    | Besuch einer weiterführenden Schule; Berufsausbildung; Studium; ich bin bereits berufstätig; ich bin auf der Suche; keine dieser Optionen |

### Q19: Liegt deine Ausbildung/ dein Studium/ deine berufliche Tätigkeit im Bereich Soziales/ Psychologie oder hast du das in Zukunft vor?

**Type:** single_choice

**Variables:** 1

| Variable | Data Type | Possible Values |
|----------|-----------|-----------------|
| Q19      | String    | Ja; Nein        |

### Q20: Gibt es etwas, das du über dein Engagement bei [U25] noch teilen möchtest?

**Type:** input_single_singleline

**Variables:** 1

| Variable | Data Type |
|----------|-----------|
| Q20      | String    |

### Q21: Zum Fragbogen selbst: War dieser für dich verständlich?

**Type:** scale

**Variables:** 1

| Variable | Data Type | Range  |
|----------|-----------|--------|
| Q21      | Int64     | [1, 5] |

### Q22: Weitere Anmerkungen?

**Type:** input_single_multiline

**Variables:** 1

| Variable | Data Type |
|----------|-----------|
| Q22      | String    |

### Q7: Wie viel Zeit verbringst du pro Monat mit [U25]?

**Type:** input_multiple_integer

**Variables:** 2

| Variable   | Label                             | Data Type |
|------------|-----------------------------------|-----------|
| Q7_stunden | Mindeste Stundenanzahl pro Monat: | Int64     |
| Q7_maximum | Maximale Stundenanzahl pro Monat: | Int64     |

