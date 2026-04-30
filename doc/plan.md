# Plan d'Amélioration de l'Algorithme de Recherche Cadastrale

Ce document détaille les tâches recommandées pour optimiser l'application de croisement entre les annonces immobilières et les données cadastrales, en tenant compte de vos critères spécifiques (Maison > 130m², Terrain > 2000m², gestion des parcelles agricoles attenantes) et de vos préférences (DPE prioritaire, pas de NLP lourd).

## 🔴 Priorité 1 : Intégration de la Base DPE (Le "Cheat Code")
*L'utilisation des données de l'ADEME est la méthode la plus fiable et rapide pour trouver une correspondance exacte sans algorithmique complexe.*

- **Tâche 1.1 : Récupération des données.** Télécharger et ingérer la base DPE Open Data de l'ADEME (format CSV ou via API) pour vos zones géographiques cibles.
- **Tâche 1.2 : Extraction des clés de jointure.** Implémenter des expressions régulières (Regex) simples pour extraire du texte de l'annonce : l'étiquette Énergie (A-G), l'étiquette Climat (GES), et potentiellement l'année de construction.
- **Tâche 1.3 : Requête de croisement.** Créer une fonction de base de données qui filtre les DPE d'une commune en utilisant la combinaison : `Surface habitable (ex: >130m² ± 5%) + Étiquette DPE + Étiquette GES`.
- **Tâche 1.4 : Récupération de l'identifiant.** Récupérer la parcelle cadastrale renvoyée par le match DPE pour l'injecter directement dans votre module `strict_match.py` [cite: 1].

## 🔴 Priorité 2 : Implémentation de la "Parcelle Ancre" et de l'Agrégation
*Indispensable pour gérer les propriétés > 2000m² composées de sols bâtis et de terres agricoles/naturelles, en s'appuyant sur vos modules existants.*

- **Tâche 2.1 : Détection de l'ancre.** Utiliser votre module `building_filter.py` [cite: 1] pour ne retenir que les parcelles possédant une emprise au sol bâtie significative (ex: > 60-70m² d'emprise cadastrale pour espérer au moins 130m² habitables avec étage).
- **Tâche 2.2 : Exclusion stricte des terrains nus isolés.** Pour réduire le bruit, exclure de la recherche initiale toutes les parcelles (agricoles, naturelles, ou urbaines) qui ne contiennent *aucun* bâtiment. Celles-ci ne seront évaluées qu'en tant qu'extensions d'une parcelle ancre.
- **Tâche 2.3 : Agrégation par adjacence.** Pour chaque "Parcelle Ancre" trouvée, faire appel à `parcel_adjacency.py` [cite: 1] pour lister toutes les parcelles limitrophes.
- **Tâche 2.4 : Validation des combos.** Dans votre flux `combo_match.py` [cite: 1], additionner la surface de l'ancre et de ses adjacentes (incluant les champs ou bois liés). Si le total correspond au critère de l'annonce (ex: > 2000m²), le groupe est conservé.

## 🟠 Priorité 3 : Extraction par Regex des Critères Stricts (Surface)
*Pour remplacer le NLP coûteux par des règles fiables et rapides.*

- **Tâche 3.1 : Regex Surface Terrain.** Créer des patterns Python pour capter les surfaces foncières. Exemple : `r"(?i)(?:terrain|parcelle|jardin|parc|clos).*?(\d[\d\s]*)\s*m[2²]"`.
- **Tâche 3.2 : Regex Surface Habitable.** Créer des patterns pour la maison. Exemple : `r"(?i)(?:maison|habitable|surface|villa).*?(\d[\d\s]*)\s*m[2²]"`.
- **Tâche 3.3 : Rejet précoce.** Implémenter un filtre qui analyse l'annonce avant tout traitement géographique : si les regex capturent explicitement un terrain de 500m², la recherche spatiale n'est même pas lancée.

## 🟡 Priorité 4 : Orchestration du Workflow (Optimisation des 2 minutes)
*Exploiter intelligemment votre budget temps de calcul pour éviter de noyer l'utilisateur sous les résultats.*

- **Tâche 4.1 : Phase 1 - DPE & Strict Match (0-5 sec).** Lancer d'abord le croisement DPE et `strict_match.py` [cite: 1]. Si un match parfait est identifié, on s'arrête là et on retourne le résultat.
- **Tâche 4.2 : Phase 2 - Combo local (5-15 sec).** En cas d'échec, exécuter l'algorithme des parcelles ancres et `combo_match.py` [cite: 1] uniquement sur la commune ciblée par l'annonce.
- **Tâche 4.3 : Phase 3 - Élargissement contrôlé (15-60 sec).** Si toujours pas de résultat clair, déléguer à `neighbor_match.py` [cite: 1] sur les communes de rang 1, puis de rang 2, en appliquant les mêmes filtres d'ancrage stricts.
- **Tâche 4.4 : Phase 4 - Système de Scoring Final.** Trier les résultats renvoyés par pertinence pour l'utilisateur :
    - *+50 points* si correspondance exacte du DPE.
    - *+30 points* si la surface du combo de parcelles est à ± 5% de la surface annoncée.
    - *-20 points* si l'emprise au sol du bâti est anormalement petite pour 130m² habitables.

## ⚪ À ignorer (Hors périmètre actuel)
- Traitement par Intelligence Artificielle (LLM/NLP) du texte des annonces (trop cher, trop lent).
- Extraction d'éléments non quantifiables ("lumineux", "calme") inexploitables géographiquement.
plan_amelioration_cadastre.md
Affichage de plan_amelioration_cadastre.md.