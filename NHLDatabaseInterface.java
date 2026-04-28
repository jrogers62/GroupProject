import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Scanner;

public class NHLDatabaseInterface {

    public static void main(String[] args) throws Exception {
        MyDatabase db = new MyDatabase();
        runConsole(db);
        System.out.println("Exiting NHL Database Interface...");
    }

    public static void runConsole(MyDatabase db) {
        Scanner console = new Scanner(System.in);
        System.out.println("==========================================");
        System.out.println("  COMP 3380 Group 16 - NHL Database CLI  ");
        System.out.println("==========================================");
        System.out.println("Welcome! Type 'h' for help.");
        System.out.print("nhl_db > ");
        String line = console.nextLine();
        String[] parts;
        String arg = "";

        while (line != null && !line.equals("q")) {
            parts = line.trim().split("\\s+");
            arg = line.contains(" ") ? line.substring(line.indexOf(" ")).trim() : "";

            switch (parts[0].toLowerCase()) {
                case "h":     printHelp();             break;
                case "reset": db.resetDatabase();      break;
                case "q1":    db.query1();             break;
                case "q2":
                    if (!arg.isEmpty()) db.query2(arg);
                    else System.out.println("Usage: q2 <player name or numeric ID>");
                    break;
                case "q3":    db.query3();             break;
                case "q4":    db.query4();             break;
                case "q5":    db.query5();             break;
                case "q6":    db.query6();             break;
                case "q7":    db.query7();             break;
                case "q8":    db.query8();             break;
                case "q9":    db.query9();             break;
                case "q10":   db.query10();            break;
                case "q11":   db.query11();            break;
                case "q12":   db.query12();            break;
                default:
                    System.out.println("Unknown command. Type 'h' for help.");
            }

            System.out.print("nhl_db > ");
            line = console.nextLine();
            arg = "";
        }

        console.close();
    }

    private static void printHelp() {
        System.out.println();
        System.out.println("--- NHL Database Commands ---");
        System.out.println("h           - Show this help menu");
        System.out.println("reset       - Delete all data from the database (repopulate with: python3 populate.py)");
        System.out.println("q1          - Top 15 goal-scorers of the 2019-20 season");
        System.out.println("q2 <query>  - Player lookup by name (partial) or numeric player ID");
        System.out.println("q3          - Multi-goal game specialists (2+ goals in one game)");
        System.out.println("q4          - Best two-way forwards by composite score (min 10 GP)");
        System.out.println("q5          - Goalies who outperformed league average save% (min 10 GP)");
        System.out.println("q6          - Goalies with most 'stolen' wins (won despite 35+ shots faced)");
        System.out.println("q7          - Best forward lines by goals (min 150 min ice time)");
        System.out.println("q8          - Most-used lines in close games (margin <= 1 goal)");
        System.out.println("q9          - Strictest/most lenient referees by avg PIM per game (min 5 GP)");
        System.out.println("q10         - Referee home-ice bias: home vs away PIM differential (min 5 GP)");
        System.out.println("q11         - Top 10 most chaotic games by total play events");
        System.out.println("q12         - Top scorer (points) per team in the 2019-20 season");
        System.out.println("q           - Exit the program");
        System.out.println("-----------------------------");
        System.out.println();
    }
}

class MyDatabase {
    private Connection connection;

public MyDatabase() {
    try {
        Class.forName("org.sqlite.JDBC");   // Force load the SQLite driver

        String url = "jdbc:sqlite:NHL_DATA_V1.6.db";

        System.out.println("DEBUG: Current directory = " + System.getProperty("user.dir"));
        System.out.println("DEBUG: Connecting to " + url);

        this.connection = DriverManager.getConnection(url);

        System.out.println("✅ Successfully connected to NHL_DATA_V1.6.db");
    } catch (Exception e) {
        System.out.println("❌ Connection failed!");
        e.printStackTrace();
    }
}

    /** Print header row and a separator of exactly the same width. */
    private static void printHeader(String fmt, Object... args) {
        String header = String.format(fmt, args).stripTrailing();
        System.out.println(header);
        System.out.println("-".repeat(header.length()));
    }

    /**
     * Pagination helper. Call once per printed data row with the current
     * 1-based row number. Every PAGE_SIZE rows it pauses and asks:
     *   N  -> print next page
     *   S  -> stop printing (return false)
     * Returns true while the caller should keep printing.
     */
    private static final int     PAGE_SIZE = 30;
    private static final Scanner PAGER     = new Scanner(System.in);

    private static boolean paginate(int rowNum) {
        if (rowNum % PAGE_SIZE != 0) return true;
        System.out.println();
        System.out.print("--- " + rowNum + " rows shown. [N] Next page  [S] Stop --- ");
        while (true) {
            String input = PAGER.nextLine().trim().toUpperCase();
            if (input.equals("N")) { System.out.println(); return true;  }
            if (input.equals("S")) { System.out.println(); return false; }
            System.out.print("    Please enter N or S: ");
        }
    }


    public void resetDatabase() {
        System.out.println("WARNING: This will delete ALL data from the database.");
        System.out.print("Are you sure? Type YES to confirm: ");
        Scanner confirm = new Scanner(System.in);
        String answer = confirm.nextLine().trim();
        if (!answer.equals("YES")) {
            System.out.println("Reset cancelled.");
            confirm.close();
            return;
        }
        
        String[] deletes = {
            "DELETE FROM \"Play_Involvement\"",
            "DELETE FROM \"Skater_Stats\"",
            "DELETE FROM \"Team_Stats\"",
            "DELETE FROM \"Line_Stats\"",
            "DELETE FROM \"Goalie_Stats\"",
            "DELETE FROM \"Officites\"",
            "DELETE FROM \"Goalie\"",
            "DELETE FROM \"Skaters\"",
            "DELETE FROM \"Lines\"",
            "DELETE FROM \"Plays\"",
            "DELETE FROM \"Referee\"",
            "DELETE FROM \"Players\"",
            "DELETE FROM \"Teams\"",
            "DELETE FROM \"Games\""
        };

        System.out.println("Deleting all data...");
        try {
            for (String sql : deletes) {
                PreparedStatement ps = connection.prepareStatement(sql);
                ps.executeUpdate();
                ps.close();
            }
            System.out.println("All data deleted successfully.");
            System.out.println("To repopulate, run: PopulateDB.java");
        } catch (SQLException e) {
            System.out.println("Error during reset:");
            e.printStackTrace(System.out);
        }
        confirm.close();
    }

    public void query1() {
        System.out.println("[Running Q1...]");
        String sql =
            "    SELECT p.playerID, p.first, p.last, p.position, " +
            "    SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals) AS goals, " +
            "    SUM(ss.assists) AS assists, " +
            "    SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals + ss.assists) AS points, " +
            "    SUM(ss.powerPlayGoals)   AS ppg, " +
            "    SUM(ss.shortHandedGoals) AS shg, " +
            "    COUNT(DISTINCT ss.gameID) AS gp " +
            "FROM Skater_Stats ss " +
            "JOIN Players p ON ss.playerID = p.playerID " +
            "WHERE CAST(ss.gameID / 1000000 AS BIGINT) = 2019 " +
            "GROUP BY ss.playerID, p.playerID, p.first, p.last, p.position " +
            "ORDER BY goals DESC, points DESC " +
            "LIMIT 15";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q1: Top 15 Goal-Scorers — 2019-20 Season ===");
            printHeader("%-4s %-22s %-4s %4s %4s %4s %4s %4s %3s",
                        "#", "Player", "Pos", "G", "A", "Pts", "PPG", "SHG", "GP");
            int rank = 1;
            while (rs.next()) {
                System.out.printf("%-4d %-22s %-4s %4d %4d %4d %4d %4d %3d%n",
                    rank++,
                    rs.getString("first") + " " + rs.getString("last"),
                    rs.getString("position"),
                    rs.getInt("goals"),
                    rs.getInt("assists"),
                    rs.getInt("points"),
                    rs.getInt("ppg"),
                    rs.getInt("shg"),
                    rs.getInt("gp"));
                if (!paginate(rank - 1)) break;
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query2(String inputArg) {
        boolean isId = inputArg.matches("\\d+");

        String cte =
            "WITH PlayerStats AS ( " +
            "    SELECT playerID, " +
            "           SUM(evenGoals + shortHandedGoals + powerPlayGoals) AS career_goals, " +
            "           SUM(assists) AS career_assists, " +
            "           0            AS career_saves, " +
            "           0            AS career_wins, " +
            "           COUNT(DISTINCT gameID) AS career_gp " +
            "    FROM Skater_Stats " +
            "    GROUP BY playerID " +
            "    UNION ALL " +
            "    SELECT playerID, " +
            "           SUM(goals) AS career_goals, " +
            "           0          AS career_assists, " +
            "           SUM(saves) AS career_saves, " +
            "           SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS career_wins, " +
            "           COUNT(DISTINCT gameID) AS career_gp " +
            "    FROM Goalie_Stats " +
            "    GROUP BY playerID " +
            ") ";

        String selectBase =
            "SELECT p.playerID, p.first, p.last, p.position, " +
            "       p.birthCountry, p.birthCity, p.birthStateProvince, " +
            "       p.birthDate, p.heightCM, p.weight, " +
            "       COALESCE(SUM(ps.career_goals),   0) AS career_goals, " +
            "       COALESCE(SUM(ps.career_assists),  0) AS career_assists, " +
            "       COALESCE(SUM(ps.career_saves),    0) AS career_saves, " +
            "       COALESCE(SUM(ps.career_wins),     0) AS career_wins, " +
            "       COALESCE(SUM(ps.career_gp),       0) AS career_gp " +
            "FROM Players p " +
            "LEFT JOIN PlayerStats ps ON p.playerID = ps.playerID ";

        String groupBy =
            "GROUP BY p.playerID, p.first, p.last, p.position, " +
            "         p.birthCountry, p.birthCity, p.birthStateProvince, " +
            "         p.birthDate, p.heightCM, p.weight ";

        String sql;
        if (isId) {
            sql = cte + selectBase + "WHERE p.playerID = ? " + groupBy;
        } else {
            sql = cte + selectBase + "WHERE (p.first || ' ' || p.last) LIKE ? " +
                  groupBy + "ORDER BY p.last, p.first";
        }

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (isId) ps.setInt(1, Integer.parseInt(inputArg));
            else      ps.setString(1, "%" + inputArg + "%");

            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q2: Player Lookup — \"" + inputArg + "\" ===");
            printHeader("%-10s %-20s %-4s %-12s %-16s %-10s %6s %4s %3s %3s %3s %5s %4s",
                        "ID", "Name", "Pos", "Country", "City/Province",
                        "Birthdate", "Ht(cm)", "Wt", "GP", "G", "A", "Saves", "Wins");

            boolean found = false;
            int rowNum2 = 0;
            while (rs.next()) {
                found = true;
                rowNum2++;
                String city = rs.getString("birthCity");
                String prov = rs.getString("birthStateProvince");
                String loc  = (city != null ? city : "") +
                              (prov != null && !prov.isEmpty() ? ", " + prov : "");
                if (loc.isEmpty()) loc = "-";
                if (loc.length() > 16) loc = loc.substring(0, 15) + ".";
                String bdate = rs.getString("birthDate");
                String bdStr = (bdate != null && bdate.length() >= 10) ? bdate.substring(0, 10) : "-";
                String name  = rs.getString("first") + " " + rs.getString("last");
                if (name.length() > 20) name = name.substring(0, 19) + ".";

                System.out.printf("%-10d %-20s %-4s %-12s %-16s %-10s %6.1f %4d %3d %3d %3d %5d %4d%n",
                    rs.getLong("playerID"),
                    name,
                    rs.getString("position"),
                    rs.getString("birthCountry"),
                    loc,
                    bdStr,
                    rs.getDouble("heightCM"),
                    rs.getInt("weight"),
                    rs.getInt("career_gp"),
                    rs.getInt("career_goals"),
                    rs.getInt("career_assists"),
                    rs.getInt("career_saves"),
                    rs.getInt("career_wins"));
                if (!paginate(rowNum2)) break;
            }
            if (!found) System.out.println("  No players found matching \"" + inputArg + "\".");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query3() {
        System.out.println("[Running Q3...]");
        String sql =
            "SELECT p.first, p.last, p.position, " +
            "    COUNT(*) AS multi_goal_games, " +
            "    SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals) AS goals_in_those_games, " +
            "    MAX(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals) AS best_single_game " +
            "FROM Skater_Stats ss " +
            "JOIN Players p ON ss.playerID = p.playerID " +
            "WHERE (ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals) >= 2 " +
            "GROUP BY ss.playerID, p.first, p.last, p.position " +
            "ORDER BY multi_goal_games DESC, goals_in_those_games DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q3: Multi-Goal Game Specialists (2+ Goals in One Game) ===");
            printHeader("%-4s %-22s %-4s %10s %22s %14s",
                        "#", "Player", "Pos", "2+G Games", "Total G (in 2+G games)", "Best Single Game");
            int rank = 1;
            while (rs.next()) {
                System.out.printf("%-4d %-22s %-4s %10d %22d %14d%n",
                    rank++,
                    rs.getString("first") + " " + rs.getString("last"),
                    rs.getString("position"),
                    rs.getInt("multi_goal_games"),
                    rs.getInt("goals_in_those_games"),
                    rs.getInt("best_single_game"));
                if (!paginate(rank - 1)) break;
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query4() {
        System.out.println("[Running Q4...]");
        String sql =
            "SELECT p.first, p.last, p.position, " +
            "    SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals) AS goals, " +
            "    SUM(ss.assists)   AS assists, " +
            "    SUM(ss.takeaways) AS tkw, " +
            "    SUM(ss.blocked)   AS blk, " +
            "    SUM(ss.plusMinus) AS plus_minus, " +
            "    SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals " +
            "        + ss.assists + ss.takeaways + ss.blocked) AS composite " +
            "FROM Skater_Stats ss " +
            "JOIN Players p ON ss.playerID = p.playerID " +
            "WHERE p.position IN ('C', 'LW', 'RW') " +
            "GROUP BY ss.playerID, p.first, p.last, p.position " +
            "HAVING COUNT(DISTINCT ss.gameID) >= 10 " +
            "ORDER BY composite DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q4: Best Two-Way Forwards (Score = G+A+Takeaways+Blocked, min 10 GP) ===");
            printHeader("%-4s %-24s %-4s %4s %4s %4s %4s %4s %9s",
                        "#", "Player", "Pos", "G", "A", "Tkw", "Blk", "+/-", "Composite");
            int rank = 1;
            while (rs.next()) {
                System.out.printf("%-4d %-24s %-4s %4d %4d %4d %4d %4d %9d%n",
                    rank++,
                    rs.getString("first") + " " + rs.getString("last"),
                    rs.getString("position"),
                    rs.getInt("goals"),
                    rs.getInt("assists"),
                    rs.getInt("tkw"),
                    rs.getInt("blk"),
                    rs.getInt("plus_minus"),
                    rs.getInt("composite"));
                if (!paginate(rank - 1)) break;
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query5() {
        System.out.println("[Running Q5...]");
        String sql =
            "SELECT p.first, p.last, " +
            "    COUNT(DISTINCT gs.gameID) AS gp, " +
            "    SUM(gs.shots) AS shots_faced, " +
            "    SUM(gs.saves) AS saves, " +
            "    ROUND(100.0 * SUM(gs.saves) / NULLIF(SUM(gs.shots), 0), 2) AS save_pct, " +
            "    ROUND( " +
            "        (100.0 * SUM(gs.saves) / NULLIF(SUM(gs.shots), 0)) " +
            "        - (SELECT 100.0 * SUM(s2.saves) / NULLIF(SUM(s2.shots), 0) FROM Goalie_Stats s2) " +
            "    , 2) AS above_avg " +
            "FROM Goalie_Stats gs " +
            "JOIN Players p ON gs.playerID = p.playerID " +
            "GROUP BY gs.playerID, p.first, p.last " +
            "HAVING COUNT(DISTINCT gs.gameID) >= 10 " +
            "ORDER BY above_avg DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q5: Goalies Outperforming League Average Save% (min 10 GP) ===");
            printHeader("%-4s %-24s %3s %11s %6s %7s %8s",
                        "#", "Goalie", "GP", "Shots Faced", "Saves", "Save%", "+/- Avg%");
            int rank = 1;
            while (rs.next()) {
                System.out.printf("%-4d %-24s %3d %11d %6d %7.2f %8.2f%n",
                    rank++,
                    rs.getString("first") + " " + rs.getString("last"),
                    rs.getInt("gp"),
                    rs.getInt("shots_faced"),
                    rs.getInt("saves"),
                    rs.getDouble("save_pct"),
                    rs.getDouble("above_avg"));
                if (!paginate(rank - 1)) break;
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query6() {
        System.out.println("[Running Q6...]");
        String sql =
            "SELECT p.first, p.last, " +
            "    COUNT(*) AS stolen_wins, " +
            "    AVG(CAST(gs.shots AS FLOAT)) AS avg_shots_in_stolen, " +
            "    MAX(gs.shots) AS max_shots_in_stolen, " +
            "    ROUND(100.0 * AVG(CAST(gs.saves AS FLOAT)) / NULLIF(AVG(CAST(gs.shots AS FLOAT)), 0), 2) AS avg_save_pct " +
            "FROM Goalie_Stats gs " +
            "JOIN Players p ON gs.playerID = p.playerID " +
            "WHERE gs.outcome = 'W' " +
            "  AND gs.shots >= 35 " +
            "GROUP BY gs.playerID, p.first, p.last " +
            "ORDER BY stolen_wins DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q6: Goalies with Most 'Stolen' Wins (Won with 35+ Shots Faced) ===");
            printHeader("%-4s %-24s %11s %14s %9s %10s",
                        "#", "Goalie", "Stolen Wins", "Avg Shots", "Max Shots", "Avg Save%");
            int rank = 1;
            boolean any = false;
            while (rs.next()) {
                any = true;
                System.out.printf("%-4d %-24s %11d %14.1f %9d %10.2f%n",
                    rank++,
                    rs.getString("first") + " " + rs.getString("last"),
                    rs.getInt("stolen_wins"),
                    rs.getDouble("avg_shots_in_stolen"),
                    rs.getInt("max_shots_in_stolen"),
                    rs.getDouble("avg_save_pct"));
                if (!paginate(rank - 1)) break;
            }
            if (!any) System.out.println("  No qualifying stolen wins found (check outcome values contain 'win').");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query7() {
        System.out.println("[Running Q7...]");
        String sql =
            "SELECT l.name AS line_name, t.teamName, t.abbreviation, " +
            "    SUM(ls.goals)    AS total_goals, " +
            "    SUM(ls.shots)    AS total_shots, " +
            "    SUM(ls.rebounds) AS total_rebounds, " +
            "    SUM(ls.iceTime)  AS total_ice_sec, " +
            "    ROUND(CAST(SUM(ls.goals) AS FLOAT) / NULLIF(SUM(ls.iceTime) / 60.0, 0) * 60, 2) AS goals_per_60, " +
            "    COUNT(DISTINCT ls.gameID) AS gp " +
            "FROM Line_Stats ls " +
            "JOIN Lines l ON ls.lineID = l.lineID " +
            "JOIN Teams t ON l.teamID  = t.teamID " +
            "GROUP BY ls.lineID, l.name, t.teamName, t.abbreviation " +
            "HAVING SUM(ls.iceTime) >= 9000 " +
            "ORDER BY total_goals DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q7: Best Forward Lines by Goals (min 150 min ice time) ===");
            printHeader("%-4s %-30s %-8s %-28s %3s %6s %8s %9s %8s %3s",
                        "#", "Line", "Team", "Full Team Name", "G", "Shots", "Rebounds",
                        "Ice(min)", "G/60min", "GP");
            int rank = 1;
            boolean any = false;
            while (rs.next()) {
                any = true;
                System.out.printf("%-4d %-30s %-8s %-28s %3d %6d %8d %9.1f %8.2f %3d%n",
                    rank++,
                    rs.getString("line_name"),
                    rs.getString("abbreviation"),
                    rs.getString("teamName"),
                    rs.getInt("total_goals"),
                    rs.getInt("total_shots"),
                    rs.getInt("total_rebounds"),
                    rs.getDouble("total_ice_sec") / 60.0,
                    rs.getDouble("goals_per_60"),
                    rs.getInt("gp"));
                if (!paginate(rank - 1)) break;
            }
            if (!any) System.out.println("  No lines found with >= 150 minutes of ice time.");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query8() {
        System.out.println("[Running Q8...]");
        String sql =
            "SELECT l.name AS line_name, t.teamName, t.abbreviation, " +
            "    COUNT(DISTINCT ls.gameID) AS close_gp, " +
            "    SUM(ls.iceTime) AS total_ice_sec, " +
            "    SUM(ls.goals)   AS goals, " +
            "    SUM(ls.shots)   AS shots " +
            "FROM Line_Stats ls " +
            "JOIN Lines l ON ls.lineID = l.lineID " +
            "JOIN Teams t ON l.teamID  = t.teamID " +
            "JOIN Games g ON ls.gameID = g.gameID " +
            "WHERE ABS(g.homeGoals - g.awayGoals) <= 1 " +
            "GROUP BY ls.lineID, l.name, t.teamName, t.abbreviation " +
            "ORDER BY total_ice_sec DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q8: Most-Used Lines in Close Games (Final Margin <= 1 Goal) ===");
            printHeader("%-4s %-30s %-8s %-28s %8s %9s %4s %5s",
                        "#", "Line", "Team", "Full Team Name",
                        "CloseGP", "Ice(min)", "G", "Shots");
            int rank = 1;
            boolean any = false;
            while (rs.next()) {
                any = true;
                System.out.printf("%-4d %-30s %-8s %-28s %8d %9.1f %4d %5d%n",
                    rank++,
                    rs.getString("line_name"),
                    rs.getString("abbreviation"),
                    rs.getString("teamName"),
                    rs.getInt("close_gp"),
                    rs.getDouble("total_ice_sec") / 60.0,
                    rs.getInt("goals"),
                    rs.getInt("shots"));
                if (!paginate(rank - 1)) break;
            }
            if (!any) System.out.println("  No data found.");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query9() {
        System.out.println("[Running Q9...]");
        String sql =
            "SELECT r.name AS referee_name, r.type AS referee_type, " +
            "    COUNT(DISTINCT o.gameID) AS games_officiated, " +
            "    ROUND(AVG(CAST(game_pim.total_pim AS FLOAT)), 2) AS avg_pim_per_game, " +
            "    SUM(game_pim.total_pim) AS total_pim_called " +
            "FROM Officites o " +
            "JOIN Referee r ON o.RID = r.RID " +
            "JOIN ( " +
            "    SELECT gameID, SUM(PIM) AS total_pim " +
            "    FROM Team_Stats " +
            "    GROUP BY gameID " +
            ") game_pim ON o.gameID = game_pim.gameID " +
            "GROUP BY o.RID, r.name, r.type " +
            "HAVING COUNT(DISTINCT o.gameID) >= 5 " +
            "ORDER BY avg_pim_per_game DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q9: Referees by Avg PIM Per Game (min 5 games, strictest first) ===");
            printHeader("%-4s %-28s %-12s %6s %12s %9s",
                        "#", "Referee", "Type", "Games", "Avg PIM/Game", "Total PIM");
            int rank = 1;
            boolean any = false;
            while (rs.next()) {
                any = true;
                System.out.printf("%-4d %-28s %-12s %6d %12.2f %9d%n",
                    rank++,
                    rs.getString("referee_name"),
                    rs.getString("referee_type"),
                    rs.getInt("games_officiated"),
                    rs.getDouble("avg_pim_per_game"),
                    rs.getInt("total_pim_called"));
                if (!paginate(rank - 1)) break;
            }
            if (!any) System.out.println("  No data found.");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query10() {
        System.out.println("[Running Q10...]");
        String sql =
            "SELECT r.name AS referee_name, r.type AS referee_type, " +
            "    COUNT(DISTINCT o.gameID) AS games_officiated, " +
            "    ROUND(AVG(CAST(home_pim.PIM AS FLOAT)), 2) AS avg_home_pim, " +
            "    ROUND(AVG(CAST(away_pim.PIM AS FLOAT)), 2) AS avg_away_pim, " +
            "    ROUND(AVG(CAST(home_pim.PIM AS FLOAT)) - AVG(CAST(away_pim.PIM AS FLOAT)), 2) AS home_minus_away " +
            "FROM Officites o " +
            "JOIN Referee r ON o.RID = r.RID " +
            "JOIN Team_Stats home_pim ON o.gameID = home_pim.gameID AND home_pim.homeOrAway = 'home' " +
            "JOIN Team_Stats away_pim ON o.gameID = away_pim.gameID AND away_pim.homeOrAway = 'away' " +
            "GROUP BY o.RID, r.name, r.type " +
            "HAVING COUNT(DISTINCT o.gameID) >= 5 " +
            "ORDER BY ABS(AVG(CAST(home_pim.PIM AS FLOAT)) - AVG(CAST(away_pim.PIM AS FLOAT))) DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q10: Referee Home-Ice Bias (Home vs Away PIM Differential, min 5 GP) ===");
            System.out.println("  Positive = more penalties on home team | Negative = favours home team");
            System.out.println();
            printHeader("%-4s %-28s %-12s %6s %10s %10s %11s",
                        "#", "Referee", "Type", "Games", "Avg Home", "Avg Away", "Home-Away");
            int rank = 1;
            boolean any = false;
            while (rs.next()) {
                any = true;
                System.out.printf("%-4d %-28s %-12s %6d %10.2f %10.2f %11.2f%n",
                    rank++,
                    rs.getString("referee_name"),
                    rs.getString("referee_type"),
                    rs.getInt("games_officiated"),
                    rs.getDouble("avg_home_pim"),
                    rs.getDouble("avg_away_pim"),
                    rs.getDouble("home_minus_away"));
                if (!paginate(rank - 1)) break;
            }
            if (!any) System.out.println("  No data found (check homeOrAway values: 'home'/'away').");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query11() {
        System.out.println("[Running Q11...]");
        String sql =
            "SELECT " +
            "    g.gameID, g.DateTimeGMT, g.type AS game_type, " +
            "    g.homeGoals, g.awayGoals, " +
            "    COUNT(pl.playID) AS total_events, " +
            "    COUNT(DISTINCT pl.type) AS event_types, " +
            "    SUM(CASE WHEN pl.type = 'GOAL'    THEN 1 ELSE 0 END) AS goal_events, " +
            "    SUM(CASE WHEN pl.type = 'PENALTY' THEN 1 ELSE 0 END) AS penalty_events, " +
            "    SUM(CASE WHEN pl.type = 'SHOT'    THEN 1 ELSE 0 END) AS shot_events " +
            "FROM Games g " +
            "JOIN Plays pl ON g.gameID = pl.gameID " +
            "GROUP BY g.gameID, g.DateTimeGMT, g.type, g.homeGoals, g.awayGoals " +
            "ORDER BY total_events DESC LIMIT 10";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q11: Top 10 Most Chaotic Games by Total Play Events ===");
            printHeader("%-4s %-12s %-19s %-5s %4s %4s %7s %6s %5s %8s %5s",
                        "#", "Game ID", "Date (GMT)", "Type",
                        "Home", "Away", "Events", "Types", "Goals", "Penalties", "Shots");
            int rank = 1;
            while (rs.next()) {
                String dt = rs.getString("DateTimeGMT");
                if (dt != null && dt.length() > 19) dt = dt.substring(0, 19);
                System.out.printf("%-4d %-12d %-19s %-5s %4d %4d %7d %6d %5d %8d %5d%n",
                    rank++,
                    rs.getLong("gameID"),
                    dt != null ? dt : "-",
                    rs.getString("game_type"),
                    rs.getInt("homeGoals"),
                    rs.getInt("awayGoals"),
                    rs.getInt("total_events"),
                    rs.getInt("event_types"),
                    rs.getInt("goal_events"),
                    rs.getInt("penalty_events"),
                    rs.getInt("shot_events"));
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }

    public void query12() {
        System.out.println("[Running Q12...]");
        String sql =
            "WITH PlayerTeamSimple AS ( " +
            "    SELECT ss.playerID, ts.teamID, " +
            "        SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals) AS goals, " +
            "        SUM(ss.assists) AS assists, " +
            "        SUM(ss.evenGoals + ss.shortHandedGoals + ss.powerPlayGoals + ss.assists) AS points, " +
            "        COUNT(DISTINCT ss.gameID) AS gp " +
            "    FROM Skater_Stats ss " +
            "    JOIN Team_Stats ts ON ss.gameID = ts.gameID " +
            "    WHERE CAST(ss.gameID / 1000000 AS BIGINT) = 2019 " +
            "    GROUP BY ss.playerID, ts.teamID " +
            "), " +
            "Ranked AS ( " +
            "    SELECT playerID, teamID, goals, assists, points, gp, " +
            "        ROW_NUMBER() OVER (PARTITION BY teamID ORDER BY points DESC, goals DESC) AS rn " +
            "    FROM PlayerTeamSimple " +
            ") " +
            "SELECT t.abbreviation, t.teamName, p.first, p.last, p.position, " +
            "    r.goals, r.assists, r.points, r.gp " +
            "FROM Ranked r " +
            "JOIN Players p ON r.playerID = p.playerID " +
            "JOIN Teams   t ON r.teamID   = t.teamID " +
            "WHERE r.rn = 1 " +
            "ORDER BY r.points DESC";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            System.out.println();
            System.out.println("=== Q12: Top Scorer Per Team — 2019-20 Season ===");
            printHeader("%-4s %-5s %-28s %-24s %-4s %3s %3s %4s %3s",
                        "#", "Team", "Team Name", "Player", "Pos", "G", "A", "Pts", "GP");
            int rank = 1;
            boolean any = false;
            while (rs.next()) {
                any = true;
                System.out.printf("%-4d %-5s %-28s %-24s %-4s %3d %3d %4d %3d%n",
                    rank++,
                    rs.getString("abbreviation"),
                    rs.getString("teamName"),
                    rs.getString("first") + " " + rs.getString("last"),
                    rs.getString("position"),
                    rs.getInt("goals"),
                    rs.getInt("assists"),
                    rs.getInt("points"),
                    rs.getInt("gp"));
                if (!paginate(rank - 1)) break;
            }
            if (!any) System.out.println("  No data found for the 2019-20 season.");
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
    }
}
