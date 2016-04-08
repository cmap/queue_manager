delete from queue_type;
insert into queue_type (name) values ("yank");
insert into queue_type (name) values ("roast");
insert into queue_type (name) values ("brew");
insert into queue_type (name) values ("PRISM fs collect");
insert into queue_type (name) values ("PRISM assemble level 2");
insert into queue_type (name) values ("S3ify");
insert into queue_type (name) values ("compare plates");
insert into queue_type (name) values ("litmus analysis");
insert into queue_type (name) values ("Cas9 cell baseline analysis");

delete from workflow_template;
insert into workflow_template (name) values ("L1000 espresso");
insert into workflow_template (name) values ("L1000 LITMUS");
insert into workflow_template (name) values ("L1000 Cas9 cell baseline analysis");
insert into workflow_template (name) values ("PRISM");

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
    where wf.name="L1000 espresso" and qt1.name="brew" and qt2.name="S3ify";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 espresso" and qt1.name="roast" and qt2.name="compare plates";

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 LITMUS" and qt1.name="roast" and qt2.name="litmus analysis";

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="L1000 Cas9 cell baseline analysis" and qt1.name="roast"
        and qt2.name="Cas9 cell baseline analysis";

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="PRISM" and qt1.name="yank" and qt2.name="PRISM fs collect";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="PRISM" and qt1.name="PRISM fs collect" and qt2.name="PRISM assemble level 2";
