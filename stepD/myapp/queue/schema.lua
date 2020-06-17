box.schema.create_space('queue',{ if_not_exists = true; })

box.space.queue:format( {
    { name = 'id';     type = 'string' },
    { name = 'status'; type = 'string' },
    { name = 'runat';  type = 'number' },
    { name = 'data';   type = '*'      },
} );

box.space.queue:create_index('primary', {
    parts = { 1, 'string' };
    if_not_exists = true;
})

box.space.queue:create_index('status', {
    parts = { 2, 'string', 1, 'string' };
    if_not_exists = true;
})

box.space.queue:create_index('runat', {
    parts = {3, 'number', 1, 'string'};
    if_not_exists = true;
})

local F = {}
for no,def in pairs(box.space.queue:format()) do
    F[no] = def.name
    F[def.name] = no
end

return {
    fields = F
}
