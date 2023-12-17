import { readFileSync, writeFileSync } from "fs";
import * as CSV from "csv";
import { stringify } from "csv-stringify/sync";
import { request, gql } from "graphql-request";

interface champDates {
  [key: string]: string;
}

const champDates = {
  // 10k
  1980: "1980-07-26", // Helsinki
  1983: "1983-08-07", // Helsinki
  1984: "1984-08-11", // Los Angeles
  1987: "1987-08-30", // Rome
  1988: "1988-09-24", // Seoul
  1991: "1991-08-26", // Tokyo
  1992: "1992-08-08", // Barcelona
  1993: "1993-08-16", // Stuttgart
  1995: "1995-08-06", // Gothenburg
  1996: "1996-08-03", // Atlanta
  1997: "1997-08-03", // Athens
  1999: "1999-08-22", // Seville
  2000: "2000-09-24", // Sydney
  2001: "2001-08-06", // Edmonton
  2003: "2003-08-23", // Saint-Denis
  2004: "2004-08-27", // Athens
  2005: "2005-08-06", // Helsinki
  2007: "2007-08-25", // Osaka
  2008: "2008-08-15", // Beijing
  2009: "2009-08-17", // Berlin
  2011: "2011-08-28", // Daegu
  2012: "2012-08-04", // London
  2013: "2013-08-10", // Moscow
  2015: "2015-08-22", // Beijing
  2016: "2016-08-12", // Rio
  2017: "2017-08-04", // London
  2019: "2019-10-06", // Doha
  2021: "2021-07-30", // Tokyo
  2022: "2022-07-17", // Eugene
  2023: "2023-08-20", // Budapest
};

// gets all results for a given athlete in a given year
const resultsQuery = gql`
  query GetSingleCompetitorResultsDiscipline(
    $getSingleCompetitorResultsDisciplineId: Int
    $resultsByYear: Int
  ) {
    getSingleCompetitorResultsDiscipline(
      id: $getSingleCompetitorResultsDisciplineId
      resultsByYear: $resultsByYear
    ) {
      resultsByEvent {
        indoor
        discipline
        results {
          date
          competition
          mark
          wind
          notLegal
        }
      }
    }
  }
`;

// gets all years in which an athlete has been active
const activityQuery = gql`
  query GetSingleCompetitor($getSingleCompetitorId: Int) {
    getSingleCompetitor(id: $getSingleCompetitorId) {
      resultsByYear {
        activeYears
      }
    }
  }
`;

// calls World Athletics graphql endpoint, URL and api key scraped from website requests
async function getActivity(id: string) {
  const activityVariables = {
    getSingleCompetitorId: id,
  };
  return await request(
    "https://wpgiegzkbrhj5mlsdxnipboepm.appsync-api.eu-west-1.amazonaws.com/graphql",
    activityQuery,
    activityVariables,
    { "X-Api-Key": "da2-juounigq4vhkvg5ac47mezxqge" }
  );
}

async function requestByAthlete(id: string, year: string) {
  const resultsVariables = {
    getSingleCompetitorResultsDisciplineId: id,
    resultsByYear: year,
  };
  for (let i = 0; i < 5; i++) {
    const data = await request(
      "https://wpgiegzkbrhj5mlsdxnipboepm.appsync-api.eu-west-1.amazonaws.com/graphql",
      resultsQuery,
      resultsVariables,
      { "X-Api-Key": "da2-juounigq4vhkvg5ac47mezxqge" }
    ).catch((err) => {
      console.error(
        "error on try " +
          i.toString() +
          " for " +
          id +
          " in " +
          year +
          ": " +
          err
      );
    });
    if (data) {
      return data;
    } else {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

// parses time as string and outputs seconds
function parseDuration(time: string) {
  if (["DNF", "DQ"].includes(time)) {
    return Infinity;
  } else {
    const [minutes, seconds] = time.split(":");
    return Number(
      (Number(minutes) * 60 + Number(seconds.replace(/[^\d]+$/, ""))).toFixed(2)
    );
  }
}

export async function GET() {
  // reads input csv
  const dataIn = CSV.parse(
    readFileSync("src/app/ALL WORLD CHAMPIONSHIPS MEN 10000.csv")
  );

  // temp storage for output values
  var dataOut = [];

  // temp storage of athlete results
  var temp: {
    [key: string]: {
      [key1: string]: {
        SB: number;
        PB: number;
        PBplus: number;
        raceCount: number;
        seasonStart: string;
      };
    };
  } = {};

  for await (const record of dataIn) {
    const [place, time, year, name, link]: string[] = record;
    if (!link) {
      continue;
    }
    // get athlete id from world athletics profile url
    const athlete_id = link.slice(-8);
    // if we haven't gotten results for this athlete yet, get them
    if (!temp[athlete_id] && athlete_id) {
      temp[athlete_id] = {};
      const years = await getActivity(athlete_id);
      // get results for this athlete starting from the beginning
      for (const i of years.getSingleCompetitor.resultsByYear.activeYears.reverse()) {
        const result = await requestByAthlete(athlete_id, i);
        if (result.getSingleCompetitorResultsDiscipline) {
          const results = result.getSingleCompetitorResultsDiscipline;

          // list of times for all disciplines
          // used for number of races and season start
          const unfilteredTimeList: {
            date: string;
            competition: string;
            mark: string;
            wind: string | null;
            notLegal: boolean;
          }[] = results.resultsByEvent
            .map((event: any) => event.results)
            .flat()
            .filter(
              (result: any) =>
                ["DNS", " - ", "", null, undefined].includes(result.mark) ===
                false
            );

          // list of times filtered to relevant event
          // used for personal and season best
          const timeList: {
            date: string;
            competition: string;
            mark: string;
            wind: string | null;
            notLegal: boolean;
          }[] =
            results.resultsByEvent
              .filter(
                (event: any) =>
                  event.discipline === "10,000 Metres" && event.indoor === false
                  // change discipline here based on event
              )[0]
              ?.results.filter(
                (result: any) =>
                  ["DNS", "VST", "EXH", " - ", "", null, undefined].includes(
                    result.mark
                  ) === false
              ) ?? [];

          // console.log(timeList);

          const raceCount = unfilteredTimeList.filter(
            (result: any) =>
              Date.parse(result?.date) < Date.parse(champDates[year])
          ).length;

          const seasonStart = unfilteredTimeList.sort(
            (a, b) => Date.parse(a.date) - Date.parse(b.date)
          )[0]?.date ?? "";

          const seasonBest = timeList.length > 0
            ? Math.min(
                ...timeList
                  .filter(
                    (result: any) =>
                      Date.parse(result?.date ?? Infinity) < Date.parse(champDates[year])
                  )
                  .map((result: any) => parseDuration(result.mark))
              )
            : Infinity;

          // console.log(seasonBest);

          // save results in temporary storage
          temp[athlete_id][i] = {
            SB: seasonBest ?? Infinity,
            PB: Math.min(
              seasonBest,
              temp[athlete_id][Number(i) - 1]?.PBplus ??
                temp[athlete_id][Number(i) - 2]?.PBplus ??
                Infinity
            ),
            // PBplus is PB for all results for the current year, including the championship or later events
            // only used to determine PB for the future years
            PBplus: Math.min( 
              ...timeList
                .map((result: any) => parseDuration(result.mark))
                .concat(
                  temp[athlete_id][i - 1]?.PBplus ??
                    temp[athlete_id][i - 2]?.PBplus ??
                    Infinity
                )
            ),
            raceCount,
            seasonStart,
          };
        } else {
          console.log("no results for " + athlete_id + " in " + i.toString());
        }
      }
      // console.log(temp[athlete_id]);
    }
    // if we have data, add it to the output
    if (temp[athlete_id][year]) {
      console.log(temp[athlete_id][year]);
      dataOut.push([
        ...record,
        temp[athlete_id][year].SB,
        temp[athlete_id][year].PB,
        temp[athlete_id][year].raceCount,
        temp[athlete_id][year].seasonStart,
      ]);
    } else {
      dataOut.push([...record, Infinity, Infinity, 0, ""]);
    }
  }

  const output = stringify(dataOut);
  // save output file
  writeFileSync("src/app/OUT Men 10000.csv", output);
  return new Response(output);
}
