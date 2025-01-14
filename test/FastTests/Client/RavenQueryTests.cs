﻿using System.Linq;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class RavenQueryTests : RavenTestBase
    {
        public RavenQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CheckIfDocumentExists()
        {
            using (var store = GetDocumentStore())
            {
                new Employees_ByFirstName().Execute(store);
                new DistributedAll().Execute(store);


                var employee1 = new NewEmployee { FirstName = "Golan", LastName = "Nahum", Address = new AddressInfo { Street = "123 Main St", } };
                var employee2 = new NewEmployee { FirstName = "Grisha", LastName = "Kotler", Address = new AddressInfo { Street = "456 Main St" } };
                using (var session = store.OpenSession())
                {
                    session.Store(employee1);
                    session.Store(employee2);
                    session.Store(new ArticleSort { Id = "ArticleSorts/1" });
                    session.Store(new DistributedPurchase { Id = "DistributedPurchases/1", ArticleSortId = "ArticleSorts/1" });

                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();


                    //Original case:
                    var distributed1 =
                        from a in session.Query<DistributedAll.Mapping, DistributedAll>().As<DistributedPurchase>()
                        let articleSort = Raven.Client.Documents.Queries.RavenQuery.Load<ArticleSort>(a.ArticleSortId)
                        select new { DistributedId = a.Id };

                    var resultsString = distributed1.ToString();

                    Assert.Equal("from index 'DistributedAll' as a load a.ArticleSortId as articleSort select { DistributedId : a.Id }", resultsString);

                    //Original case using RavenQuery.Id:
                    var distributed2 =
                        from a in session.Query<DistributedAll.Mapping, DistributedAll>().As<DistributedPurchase>()
                        let articleSort = Raven.Client.Documents.Queries.RavenQuery.Load<ArticleSort>(a.ArticleSortId)
                        select new { DistributedId = Raven.Client.Documents.Queries.RavenQuery.Id(a) };

                    var results2 = distributed2.ToList();
                    var resultsString2 = distributed2.ToString();

                    Assert.Equal("from index 'DistributedAll' as a load a.ArticleSortId as articleSort select { DistributedId : id(a) }", resultsString2);
                    Assert.Equal("DistributedPurchases/1", results2[0].DistributedId);

                    //Test 1:
                    var queryUnnamedClass = session.Query<Employees_ByFirstName.IndexEntry, Employees_ByFirstName>()
                        .Select(a => new { FirstName = a.FirstName, LastName = a.LastName, EmployeeId = Raven.Client.Documents.Queries.RavenQuery.Id(a) });

                    var queryUnnamedClassString = queryUnnamedClass.ToString();
                    var returnedEmployees1 = queryUnnamedClass.ToList();
                    var returnedEmployee11 = returnedEmployees1.First();
                    var returnedEmployee12 = returnedEmployees1.Last();

                    Assert.Equal(
                        "from index 'Employees/ByFirstName' select FirstName, LastName, id() as EmployeeId",
                        queryUnnamedClassString);
                    Assert.Equal(2, returnedEmployees1.Count);

                    Assert.Equal(employee1.Id, returnedEmployee11.EmployeeId);
                    Assert.Equal(employee1.FirstName, returnedEmployee11.FirstName);
                    Assert.Equal(employee1.LastName, returnedEmployee11.LastName);
                    Assert.Equal(employee2.Id, returnedEmployee12.EmployeeId);
                    Assert.Equal(employee2.FirstName, returnedEmployee12.FirstName);
                    Assert.Equal(employee2.LastName, returnedEmployee12.LastName);


                    //Test 2:
                    var queryUnnamedClass2 = session.Query<Employees_ByFirstName.IndexEntry, Employees_ByFirstName>()
                        .Select(a => new { FullName = a.FirstName + " " + a.LastName, EmployeeId = Raven.Client.Documents.Queries.RavenQuery.Id(a) });

                    var queryUnnamedClass2String = queryUnnamedClass2.ToString();
                    var returnedEmployees2 = queryUnnamedClass2.ToList();
                    var returnedEmployee21 = returnedEmployees2.First();
                    var returnedEmployee22 = returnedEmployees2.Last();

                    Assert.Equal(
                        "from index 'Employees/ByFirstName' as a select { FullName : a.FirstName+\" \"+a.LastName, EmployeeId : id(a) }",
                        queryUnnamedClass2String);
                    Assert.Equal(2, returnedEmployees2.Count);

                    Assert.Equal(employee1.FirstName + " " + employee1.LastName, returnedEmployee21.FullName);
                    Assert.Equal(employee1.Id, returnedEmployee21.EmployeeId);
                    Assert.Equal(employee2.FirstName + " " + employee2.LastName, returnedEmployee22.FullName);
                    Assert.Equal(employee2.Id, returnedEmployee22.EmployeeId);


                    //Test 3:
                    var queryNamedClass = session.Query<Employees_ByFirstName.IndexEntry, Employees_ByFirstName>()
                        .Select(a => new EmployeeProjection { FullName = a.FirstName + " " + a.LastName, EmployeeId = Raven.Client.Documents.Queries.RavenQuery.Id(a) });

                    var queryNamedClassString = queryNamedClass.ToString();
                    var returnedEmployees3 = queryNamedClass.ToList();
                    var returnedEmployee31 = returnedEmployees3.First();
                    var returnedEmployee32 = returnedEmployees3.Last();

                    Assert.Equal(
                        "from index 'Employees/ByFirstName' as a select { FullName : a.FirstName+\" \"+a.LastName, EmployeeId : id(a) }",
                        queryNamedClassString);
                    Assert.Equal(2, returnedEmployees3.Count);

                    Assert.Equal(employee1.Id, returnedEmployee31.EmployeeId);
                    Assert.Equal(employee1.FirstName + " " + employee1.LastName, returnedEmployee31.FullName);
                    Assert.Equal(employee2.Id, returnedEmployee32.EmployeeId);
                    Assert.Equal(employee2.FirstName + " " + employee2.LastName, returnedEmployee32.FullName);


                    //Test 4:
                    var queryNamedClassWithNestedClass = session.Query<NewEmployee>()
                        .Select(a => new EmployeeProjection
                        {
                            EmployeeId = Raven.Client.Documents.Queries.RavenQuery.Id(a),
                            FullName = a.FirstName + " " + a.LastName,
                            Address = new AddressInfo { AddressId = Raven.Client.Documents.Queries.RavenQuery.Id(a), }
                        });

                    var queryNamedClassWithNestedClassString = queryNamedClassWithNestedClass.ToString();
                    var returnedEmployees4 = queryNamedClassWithNestedClass.ToList();
                    var returnedEmployee41 = returnedEmployees4.First();
                    var returnedEmployee42 = returnedEmployees4.Last();

                    Assert.Equal(
                        "from 'NewEmployees' as a select { EmployeeId : id(a), FullName : a.FirstName+\" \"+a.LastName, Address : { AddressId : id(a) } }",
                        queryNamedClassWithNestedClassString);
                    Assert.Equal(2, returnedEmployees4.Count);

                    Assert.Equal(employee1.Id, returnedEmployee41.EmployeeId);
                    Assert.Equal(employee1.FirstName + " " + employee1.LastName, returnedEmployee41.FullName);
                    Assert.Equal(employee1.Id, returnedEmployee41.Address.AddressId);
                    Assert.Equal(employee2.Id, returnedEmployee42.EmployeeId);
                    Assert.Equal(employee2.FirstName + " " + employee2.LastName, returnedEmployee42.FullName);
                    Assert.Equal(employee2.Id, returnedEmployee42.Address.AddressId);


                    // Test 5:
                    var query = session.Query<Employees_ByFirstName.IndexEntry, Employees_ByFirstName>().As<NewEmployee>()
                        .Select(e => new { Employee = e })
                        .Select(employee => new EmployeeProjection() { EmployeeId = Raven.Client.Documents.Queries.RavenQuery.Id(employee.Employee) });

                    var returnedEmployees5 = query.ToList();
                    var queryString = query.ToString();
                    var returnedEmployee51 = returnedEmployees5.First();
                    var returnedEmployee52 = returnedEmployees5.Last();

                    Assert.Equal(employee1.Id, returnedEmployee51.EmployeeId);
                    Assert.Equal(employee2.Id, returnedEmployee52.EmployeeId);
                }
            }
        }


        private class DistributedAll : AbstractMultiMapIndexCreationTask<DistributedAll.Mapping>
        {
            public class Mapping
            {
                public string Id { get; set; }
            }

            public DistributedAll()
            {
                AddMap<DistributedPurchase>(results => from result in results
                    select new Mapping { Id = result.Id });
            }
        }

        private class DistributedPurchase
        {
            public string Id { get; set; }

            public string ArticleSortId { get; set; }
        }

        private class Distributed
        {
            public string Id { get; set; }
        }

        private class ArticleSort
        {
            public string Id { get; set; }
        }

        private class Employees_ByFirstName : AbstractIndexCreationTask<NewEmployee>
        {
            public Employees_ByFirstName()
            {
                Map = employees => from employee in employees
                    select new { employee.FirstName };
            }

            public class IndexEntry
            {
                public string Id { get; set; }
                public string FirstName { get; set; }
                public string LastName { get; set; }
            }
        }

        private class EmployeeProjection
        {
            public string EmployeeId { get; set; }
            public string FullName { get; set; }
            public AddressInfo Address { get; set; }
        }

        private class NewEmployee : Employee
        {
            public AddressInfo Address { get; set; }
        }

        private class AddressInfo
        {
            public string Street { get; set; }
            public string AddressId { get; set; }
            public AddressInfo[] Details { get; set; }
        }
    }
}
