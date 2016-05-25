delete from queue_type;
insert into queue_type (name) values ("yank");
insert into queue_type (name) values ("roast");
insert into queue_type (name) values ("brew");
insert into queue_type (name) values ("PRISM fs collect");
insert into queue_type (name) values ("PRISM assemble level 2");
insert into queue_type (name) values ("S3ify");
insert into queue_type (name) values ("compare plates");
insert into queue_type (name) values ("litmusify");
insert into queue_type (name) values ("Cas9 cell baseline analysis");

delete from workflow_template;
insert into workflow_template (name) values ("L1000 espresso");
insert into workflow_template (name) values ("L1000 LITMUS");

delete from workflow_template_pair;
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 espresso" and qt1.name="yank" and qt2.name="roast";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 espresso" and qt1.name="roast" and qt2.name="brew";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 espresso" and qt1.name="brew" and qt2.name="compare plates";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 espresso" and qt1.name="compare plates" and qt2.name="S3ify";

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 LITMUS" and qt1.name="roast" and qt2.name="litmusify";
