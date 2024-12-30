SELECT 
    pci.id                  AS "ID",
    pci.post_id             AS "Post ID",
    aci.id                  AS "Item ID",
    aci.label               AS "Item Label",
    aci.i18n                AS "Item i18n",
    acl.id                  AS "Checklist ID",
    acl.title               AS "Checklist Title"
FROM post_checklist_item pci
    LEFT JOIN adaptive_checklist_item aci
        ON pci.item_id = aci.id
    LEFT JOIN adaptive_checklist acl 
        ON aci.checklist_id = acl.id
ORDER BY "Post ID", "Checklist ID", "Item ID"