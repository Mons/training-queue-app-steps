box.schema.user.create('queue', { if_not_exists = true })
box.schema.user.passwd('queue', 'queue')
box.schema.user.grant('queue', 'super', nil, nil, { if_not_exists = true })
