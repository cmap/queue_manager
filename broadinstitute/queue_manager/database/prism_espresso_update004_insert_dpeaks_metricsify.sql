insert into queue_type (name) values ("dpeak_metricsify");

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 espresso" and qt1.name="brew" and qt2.name="dpeak_metricsify";
