import { autocomplete } from "../autocompleteUtils";
import { AUTOCOMPLETE_META } from "../../src/providers/common";

describe("can complete fields", function () {
    
    describe("from collection", function () {
        it("can complete in group by - w/o prefix", async function () {
            const suggestions = await autocomplete("from Orders group by | ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Company", "Employee"]);
        });

        it("can complete in group by - w/ prefix", async function () {
            const suggestions = await autocomplete("from Orders as o group by o.| ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Company", "Employee"]);
        });

        it("can complete nested field in group by - w/ prefix", async function () {
            const suggestions = await autocomplete("from Orders as o group by o.Lines[].| ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["PricePerUnit", "Quantity"]);
        });

        it("can complete partial in group by ", async function () {
            const suggestions = await autocomplete("from Orders group by Co|");
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Company"]);

            expect(suggestions.map(x => x.caption))
                .not.toIncludeAllMembers(["Employee"]);
        });

        it("can complete nested path in group by ", async () => {
            const suggestions = await autocomplete(" from Orders group by array(Lines[].|), ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["PricePerUnit", "Discount"]);
        });

        it("can complete inner fields in group by - with spaces ", async function () {
            const suggestions = await autocomplete("from Orders group by Lines   .   |");
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["PricePerUnit", "Discount"]);
        });

        it("can complete inner fields in group by - with out spaces ", async function () {
            const suggestions = await autocomplete("from Orders group by Lines.|  ");
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["PricePerUnit", "Discount"]);
        });

        it("can complete after comma - root", async function () {
            const suggestions = await autocomplete("from Orders group by Field1, Lines.|  ");
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["PricePerUnit", "Discount"]);
        });

        it("can complete after comma - inner", async function () {
            const suggestions = await autocomplete("from Orders group by Field1, |  ");
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Lines", "Company"]);
        });

        it("doesn't complete when comma is missing", async function () {
            const suggestions = await autocomplete("from Orders group by Field1 | ");

            // should contain keywords
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["as", "limit", "select"]);

            // no fields
            expect(suggestions.filter(x => x.meta === AUTOCOMPLETE_META.field))
                .toHaveLength(0);
        });

        it("can complete next field when no space after comma", async () => {
            const suggestions = await autocomplete(" from Orders group by ShipTo,|");

            // should NOT contain keywords
            expect(suggestions.map(x => x.caption))
                .not.toIncludeAllMembers(["as", "limit", "select"]);

            const fields = ["Company", "Lines"];

            for (const field of fields) {
                const matchingItem = suggestions.find(x => x.value.startsWith(field));
                expect(matchingItem)
                    .toBeTruthy();
            }
        });
    });
    
    describe("from index", function () {
        it("doesn't suggest group by on index", async function () {
            const suggestions = await autocomplete("from index 'Orders/ByCompany' | ");
            
            expect(suggestions.find(x => x.value.startsWith("group by")))
                .toBeFalsy();
        });
        
        
        it("can complete index fields - w/o prefix", async function() {
            const suggestions = await autocomplete("from index 'Orders/ByCompany' select | ");
            
            const indexFields = ["Company", "Count", "Total"];
            
            for (const field of indexFields) {
                expect(suggestions.find(x => x.value.startsWith(field)))
                    .toBeTruthy();
            }
        });

        it("can complete index fields - w/ prefix", async function() {
            const suggestions = await autocomplete("from index 'Orders/ByCompany' as iii select iii.| ");

            const indexFields = ["Company", "Count", "Total"];

            for (const field of indexFields) {
                expect(suggestions.find(x => x.value.startsWith(field)))
                    .toBeTruthy();
            }
        });
        
        it("can complete fields with prefix when prefix is defined", async function() {
            const suggestions = await autocomplete("from index 'Orders/ByCompany' as iii where |");

            const indexFields = ["Company", "Count", "Total"];

            for (const field of indexFields) {
                const suggestionWithPrefix = suggestions.find(x => x.value.startsWith("iii." + field));
                expect(suggestionWithPrefix)
                    .toBeTruthy();
                expect(suggestionWithPrefix.caption)
                    .toStartWith("iii.");
                
                // we should only suggest fields with prefix
                expect(suggestions.find(x => x.value.startsWith(field)))
                    .toBeFalsy();
            }
        });
    });
});
