package com.xxx.rpc.test;


import org.neo4j.driver.v1.*;


import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by MK33 on 2016/12/30.
 */
public class Noe4jTest
{

    public static void main(String[] args) {

        AuthToken token = AuthTokens.basic("neo4j", "make");
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", token);

        try (Session session = driver.session())
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (a:Person {name: {name}, title: {title}})",
                        parameters( "name", "Arthur", "title", "King" ) );
                tx.success();
            }
            try ( Transaction tx = session.beginTransaction() )
            {
                StatementResult result = tx.run( "MATCH (a:Person) WHERE a.name = {name} " +
                                "RETURN a.name AS name, a.title AS title",
                        parameters( "name", "Arthur" ) );
                while ( result.hasNext() )
                {
                    Record record = result.next();
                    System.out.println( String.format( "%s %s", record.get( "title" ).asString(), record.get( "name" ).asString() ) );
                }
            }
        }
        driver.close();
    }


    public static void println(Object o) {
        int i;
        /*
        * */

        /***/
        System.out.println(o);
    }
}
