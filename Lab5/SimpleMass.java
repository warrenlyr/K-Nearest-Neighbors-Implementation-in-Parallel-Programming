package edu.uwb.css534;

import java.util.Date;

import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;

public class SimpleMass {
    private static final String NODE_FILE = "nodes.xml";
	
	@SuppressWarnings("unused")		// some unused variables left behind for easy debugging

    public static void main(String[] args) {
		// remember starting time
		long startTime = new Date().getTime();
		
		// init MASS library
		MASS.setNodeFilePath( NODE_FILE );
		MASS.setLoggingLevel( LogLevel.DEBUG );
		
		// start MASS
		MASS.getLogger().debug( "Quickstart initializing MASS library..." );
		MASS.init();
		MASS.getLogger().debug( "MASS library initialized" );

        MASS.getLogger().debug( "Quickstart creating Places..." );
		Places places = new Places( 1, Matrix.class.getName(), ( Object ) new Integer( 0 ), 5, 5);
        MASS.getLogger().debug("Places created");
        
		MASS.getLogger().debug( "Quickstart creating Agents..." );
		Agents agents = new Agents( 2, Nomad.class.getName(), null, places, 5 );
        MASS.getLogger().debug("Agents created");
        
        // tell Agents to move
        MASS.getLogger().debug( "Quickstart instructs all Agents to migrate..." );
        agents.callAll(Nomad.MIGRATE);
        MASS.getLogger().debug( "Agent migration complete" );
        
        // sync all Agent status
        MASS.getLogger().debug( "Quickstart sending manageAll to Agents..." );
        agents.manageAll();
		MASS.getLogger().debug("Agents manageAll operation complete");
		
		// find out where they live now
		Object[] agentsCallAllObjs = new Object[25];
		MASS.getLogger().debug( "Quickstart sending callAll to Agents..." );
		Object[] calledAgentsResults = ( Object[] ) agents.callAll( Nomad.GET_HOSTNAME, agentsCallAllObjs );
		MASS.getLogger().debug( "Agents callAll operation complete" );
        
        // orderly shutdown
		MASS.getLogger().debug( "Quickstart instructs MASS library to finish operations..." );
		MASS.finish();
		MASS.getLogger().debug("MASS library has stopped");
		
		// calculate / display execution time
		long execTime = new Date().getTime() - startTime;
		System.out.println( "Execution time = " + execTime + " milliseconds" );
    }
}
