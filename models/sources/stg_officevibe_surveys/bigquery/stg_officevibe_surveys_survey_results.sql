{{config(enabled = target.type == 'bigquery')}}
{% if var("hr_warehouse_survey_sources") %}
{% if 'officevibe_surveys' in var("hr_warehouse_survey_sources") %}

with source as (

    select * from {{ var('stg_officevibe_surveys_results_table') }}

),
renamed as (
  SELECT
  `Date` as survey_ts,
  safe_cast(Engagement as float64) as engagement_score,
  safe_cast(Participation____ as float64) as overall_participation_score,
  safe_cast(eNPS as float64) as e_nps,
  safe_cast(Recognition as float64) as overall_recognition_score,
  safe_cast(Ambassadorship as float64) as overall_ambassadorship_score,
  safe_cast(Feedback as float64) as overall_feedback_score,
  safe_cast(Relationship_with_Peers as float64) as relationship_with_peers_score,
  safe_cast(Relationship_with_Manager as float64) as relationship_with_manager_score,
  safe_cast(Satisfaction as float64) as overall_satisfaction_score,
  safe_cast(Alignment as float64) as overall_alignment_score,
  safe_cast(Happiness as float64) as overall_happiness_score,
  safe_cast(Wellness as float64) as overall_wellness_score,
  safe_cast(Personal_Growth as float64) as overall_personal_growth_score,
  safe_cast(Recognition___Recognition_Quality as float64) as recognition_quality_score,
  safe_cast(Recognition___Recognition_Frequency as float64) as recognition_frequency_score,
  safe_cast(Ambassadorship___Championing as float64) as ambassadorship_championing_score,
  safe_cast(Ambassadorship___Pride as float64) as ambassadorship_pride_score,
  safe_cast(Feedback___Feedback_Quality as float64) as feedback_quality_score,
  safe_cast(Feedback___Feedback_Frequency as float64) as feedback_frequency_score,
  safe_cast(Feedback___Suggestions_for_the_Organization as float64) as feedback_suggestions_score,
  safe_cast(Relationship_with_Peers___Collaboration_between_Peers as float64) as relationship_collaboration_between_peers_score,
  safe_cast(Relationship_with_Peers___Trust_between_Peers as float64) as relationship_trust_between_peers_score,
  safe_cast(Relationship_with_Peers___Communication_between_Peers as float64) as relationship_communication_between_peers_score,
  safe_cast(Relationship_with_Manager___Collaboration_with_Manager as float64) as relatiohip_with_manager_score,
  safe_cast(Relationship_with_Manager___Trust_with_Manager as float64) as relationship_trust_with_manager_score,
  safe_cast(Relationship_with_Manager___Communication_with_Manager as float64) as relationship_communication_with_manager_score,
  safe_cast(Satisfaction___Fairness as float64) as satisfaction_fairness_score,
  safe_cast(Satisfaction___Role_within_Organization as float64) as satisfaction_role_within_organization_score,
  safe_cast(Satisfaction___Work_environment as float64) as satisfaction_work_environment_score,
  safe_cast(Alignment___Values as float64) as alignment_values_score,
  safe_cast(Alignment___Vision___Mission as float64) as alignment_vision_score,
  safe_cast(Alignment___Ethics___Social_Responsibility as float64) as alignment_ethics_score,
  safe_cast(Happiness___Happiness_at_Work as float64) as happiness_at_work_score,
  safe_cast(Happiness___Work_Life_Balance as float64) as happiness_work_life_balance_score,
  safe_cast(Wellness___Stress as float64) as wellness_stress_score,
  safe_cast(Wellness___Personal_Health as float64) as wellness_personal_health_score,
  safe_cast(Personal_Growth___Autonomy as float64) as personal_growth_autonomy_score,
  safe_cast(Personal_Growth___Mastery as float64) as personal_growth_mastery_score,
  safe_cast(Personal_Growth___Purpose as float64) as personal_growth_purpose_score
FROM
 source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
